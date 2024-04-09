(ns witan.send.adroddiad.analysis.total-ehcp-projection
  (:require
   [ham-fisted.reduce :as hf-reduce]
   [tablecloth.api :as tc]
   [tech.v3.dataset.reductions :as ds-reduce]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.datatype.gradient :as gradient]
   [witan.send :as ws]
   [witan.send.adroddiad.summary-v2.io :as sio]
   [witan.send.adroddiad.transitions :as tr]))

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
  [sim {:keys [ks historic-transitions-count background-population]}]
  (-> (tc/concat-copying historic-transitions-count sim)
      (tr/transitions->census)
      (tc/group-by ks)
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (add-diff :transition-count)
      (tc/rename-columns
       {:diff :ehcp-diff
        :pct-diff :ehcp-pct-diff})
      (tc/inner-join background-population [:calendar-year])
      (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
      (tc/order-by ks)))

(defn summarise
  [simulation-results
   {:keys [background-population historic-transitions-count simulation-count grouping-keys]
    :or {grouping-keys [:calendar-year]}}]
  (let [summary
        (tc/order-by
         (->> simulation-results
              (hf-reduce/preduce
               ;; init-val
               (fn [] [])
               ;; rfn
               (fn [acc sim]
                 (conj acc
                       (transform-simulation
                        sim
                        {:background-population background-population
                         :historic-transitions-count historic-transitions-count
                         :ks grouping-keys})))
               ;; merge-fn
               (fn [acc acc']
                 (into acc acc')))
              (ds-reduce/group-by-column-agg
               grouping-keys
               {:transition-count-summary
                (percentiles-reducer simulation-count :transition-count)
                :echp-diff-summary
                (percentiles-reducer simulation-count :ehcp-diff)
                :ehcp-pct-diff-summary
                (percentiles-reducer simulation-count :ehcp-pct-diff)
                :pct-ehcps-summary
                (percentiles-reducer simulation-count :pct-ehcps)}))
         grouping-keys)]
    {:transition-count-summary
     (-> summary
         (tc/select-columns [:calendar-year :transition-count-summary])
         (tc/separate-column :transition-count-summary :infer identity))
     :echp-diff-summary
     (-> summary
         (tc/select-columns [:calendar-year :echp-diff-summary])
         (tc/separate-column :echp-diff-summary :infer identity))
     :ehcp-pct-diff-summary
     (-> summary
         (tc/select-columns [:calendar-year :ehcp-pct-diff-summary])
         (tc/separate-column :ehcp-pct-diff-summary :infer identity))
     :pct-ehcps-summary
     (-> summary
         (tc/select-columns [:calendar-year :pct-ehcps-summary])
         (tc/separate-column :pct-ehcps-summary :infer identity))}))


#_
(time
 (def baz
   ;; This takes 10 seconds and hits 50% of the cores
   (let [ks [:calendar-year]
         background-population (-> (:data total-background-population)
                                   (tc/rename-columns
                                    {:source :tbp-source
                                     :diff :tbp-diff
                                     :pct-diff :tbp-pct-diff}))
         reducer (ds-reduce/group-by-column-agg-rf
                  ks
                  {:transition-count-summary
                   (percentiles-reducer simulation-count :transition-count)
                   :echp-diff-summary
                   (percentiles-reducer simulation-count :ehcp-diff)
                   :ehcp-pct-diff-summary
                   (percentiles-reducer simulation-count :ehcp-pct-diff)
                   :pct-ehcps-summary
                   (percentiles-reducer simulation-count :pct-ehcps)})
         reducer-xform (hf-reduce/reducer-xform->reducer
                        reducer
                        (comp
                         (map (fn [sim]
                                (-> (tc/concat-copying historic-transitions sim)
                                    (tr/transitions->census)
                                    (tc/group-by ks)
                                    (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                                    (add-diff :transition-count)
                                    (tc/rename-columns
                                     {:diff :ehcp-diff
                                      :pct-diff :ehcp-pct-diff})
                                    (tc/inner-join background-population [:calendar-year])
                                    (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
                                    (tc/order-by ks))))))]

     (hf-reduce/reduce-reducer
      reducer-xform
      simulation-results))))
