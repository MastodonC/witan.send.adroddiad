(ns witan.send.adroddiad.analysis.total-settings
  (:require
   [ham-fisted.reduce :as hf-reduce]
   [tablecloth.api :as tc]
   [tech.v3.dataset.reductions :as ds-reduce]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.datatype.gradient :as gradient]
   [witan.send :as ws]
   [witan.send.adroddiad.analysis.total-background-population :as tbp]
   [witan.send.adroddiad.summary-v2 :as summary]
   [witan.send.adroddiad.summary-v2.io :as sio]
   [witan.send.adroddiad.transitions :as tr]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]))

(defn transitions-from-config [config-edn]
  (let [config (ws/read-config config-edn)]
    (tc/dataset
     (str (:project-dir config) "/" (get-in config [:file-inputs :transitions]))
     {:key-fn keyword})))

(defn historic-ehcp-count [transitions]
  (-> transitions
      (tc/group-by
       [:calendar-year
        :setting-1 :need-1 :academic-year-1
        :setting-2 :need-2 :academic-year-2])
      (tc/aggregate {:transition-count tc/row-count})
      (tc/map-columns :calendar-year-2 [:calendar-year] (fn [^long cy] (inc cy)))))

(defn simulation-data-from-config [config-edn prefix]
  (->> config-edn
       ws/read-config
       :project-dir
       (sio/simulated-transitions-files prefix)
       (sio/files->ds-vec)))

(defn add-diff [ds value-col]
  (let [ds' (tc/order-by ds [:calendar-year])
        diff (gradient/diff1d (value-col ds'))
        values (value-col ds')
        pct-diff (sequence
                  (map (fn [d m] (dfn// d m)))
                  diff values)]
    (-> ds'
        (tc/add-column :diff (into [0] diff))
        (tc/add-column :pct-diff (into [0] pct-diff)))))

(defn percentiles-reducer [simulation-count value-key]
  (ds-reduce/reducer
   value-key
   ;; init-val
   (fn [] [])
   ;; rfn
   (fn [xs x] (conj xs x))
   ;; merge-fn
   (fn [v1 v2] (into v1 v2))
   ;; finaliser
   (fn [xs]
     (let [observations (count xs)]
       (-> (zipmap
            [:min :p05 :q1 :median :q3 :p95 :max]
            (dfn/percentiles
             (into
              (vec (repeat
                    (- simulation-count observations)
                    0))
              xs)
             [0.00001 5 25 50 75 95 100]))
           (assoc :observations observations))))))

(defn transform-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (-> census
        (tc/group-by numerator-grouping-keys)
        (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
        (add-diff :transition-count)
        (tc/rename-columns
         {:diff :ehcp-diff
          :pct-diff :ehcp-pct-diff})
        (tc/inner-join denominator denominator-grouping-keys)
        (tc/map-columns :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
        (tc/order-by numerator-grouping-keys))))

(defn summarise
  [simulation-results
   {:keys [historic-transitions-count simulation-count numerator-grouping-keys denominator-grouping-keys]
    :or {numerator-grouping-keys [:calendar-year :setting]
         denominator-grouping-keys [:calendar-year]}}]
  (let [summary
        (tc/order-by
         (->> simulation-results
              (hf-reduce/preduce
               ;; init-val
               (fn [] [])
               ;; rfn
               (fn [acc sim]
                 (conj acc
                       (try
                         (transform-simulation
                          sim
                          {:denominator-grouping-keys denominator-grouping-keys
                           :historic-transitions-count historic-transitions-count
                           :numerator-grouping-keys numerator-grouping-keys})
                         (catch Exception e (throw (ex-info "Failed to transform simulation."
                                                            {:sim sim
                                                             :denominator-grouping-keys denominator-grouping-keys
                                                             :historic-transitions-count historic-transitions-count
                                                             :numerator-grouping-keys numerator-grouping-keys}
                                                            e))))))
               ;; merge-fn
               (fn [acc acc']
                 (into acc acc')))
              (ds-reduce/group-by-column-agg
               numerator-grouping-keys
               {:transition-count-summary
                (percentiles-reducer simulation-count :transition-count)
                :echp-diff-summary
                (percentiles-reducer simulation-count :ehcp-diff)
                :ehcp-pct-diff-summary
                (percentiles-reducer simulation-count :ehcp-pct-diff)
                :pct-ehcps-summary
                (percentiles-reducer simulation-count :pct-ehcps)}))
         numerator-grouping-keys)]
    {:transition-count-summary
     {:table
      (-> summary
          (tc/select-columns (conj numerator-grouping-keys :transition-count-summary))
          (tc/separate-column :transition-count-summary :infer identity))}
     :echp-diff-summary
     {:table
      (-> summary
          (tc/select-columns (conj numerator-grouping-keys :echp-diff-summary))
          (tc/separate-column :echp-diff-summary :infer identity))}
     :ehcp-pct-diff-summary
     {:table
      (-> summary
          (tc/select-columns (conj numerator-grouping-keys :ehcp-pct-diff-summary))
          (tc/separate-column :ehcp-pct-diff-summary :infer identity))}
     :pct-ehcps-summary
     {:table
      (-> summary
          (tc/select-columns (conj numerator-grouping-keys :pct-ehcps-summary))
          (tc/separate-column :pct-ehcps-summary :infer identity))}}))

(defn summarise-from-config [config-edn pqt-prefix]
  (let [cfg (-> (ws/read-config config-edn))]
    (summarise
     (simulation-data-from-config config-edn pqt-prefix)
     {:historic-transitions-count (-> config-edn
                                      transitions-from-config
                                      historic-ehcp-count)
      :simulation-count (get-in cfg [:projection-parameters
                                     :simulations])})))
