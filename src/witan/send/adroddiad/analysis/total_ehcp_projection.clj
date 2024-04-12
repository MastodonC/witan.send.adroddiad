(ns witan.send.adroddiad.analysis.total-ehcp-projection
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
     {:table
      (-> summary
          (tc/select-columns [:calendar-year :transition-count-summary])
          (tc/separate-column :transition-count-summary :infer identity))}
     :echp-diff-summary
     {:table
      (-> summary
          (tc/select-columns [:calendar-year :echp-diff-summary])
          (tc/separate-column :echp-diff-summary :infer identity))}
     :ehcp-pct-diff-summary
     {:table
      (-> summary
          (tc/select-columns [:calendar-year :ehcp-pct-diff-summary])
          (tc/separate-column :ehcp-pct-diff-summary :infer identity))}
     :pct-ehcps-summary
     {:table
      (-> summary
          (tc/select-columns [:calendar-year :pct-ehcps-summary])
          (tc/separate-column :pct-ehcps-summary :infer identity))}}))

(defn summarise-from-config [config-edn pqt-prefix]
  (let [cfg (-> (ws/read-config config-edn))]
    (summarise
     (simulation-data-from-config config-edn pqt-prefix)
     {:historic-transitions-count (-> config-edn
                                      transitions-from-config
                                      historic-ehcp-count)
      :simulation-count (get-in cfg [:projection-parameters
                                     :simulations])
      :background-population (tbp/population-from-config config-edn)})))

(defn transition-count-summary-map [transition-count-summary {:keys [anchor-year]}]
  (let [anchor-year (or anchor-year (reduce min (:calendar-year transition-count-summary)))
        five-year (+ 5 anchor-year)
        ten-year (+ 10 anchor-year)
        anchor-year-median (summary/value-at transition-count-summary #(= anchor-year (:calendar-year %)) :median)
        five-year-median (summary/value-at transition-count-summary #(= five-year (:calendar-year %)) :median)
        ten-year-median (summary/value-at transition-count-summary #(= ten-year (:calendar-year %)) :median)
        five-year-delta (dfn/- five-year-median anchor-year-median)
        five-year-delta-pct (float (dfn// five-year-delta anchor-year-median))
        ten-year-delta (dfn/- ten-year-median anchor-year-median)
        ten-year-delta-pct (float (dfn// ten-year-delta anchor-year-median))]
    {:anchor-year anchor-year
     :five-year five-year
     :ten-year ten-year
     :anchor-year-median anchor-year-median
     :five-year-median five-year-median
     :ten-year-median ten-year-median
     :five-year-delta five-year-delta
     :five-year-delta-pct five-year-delta-pct
     :ten-year-delta ten-year-delta
     :ten-year-delta-pct ten-year-delta-pct}))

(defn transition-count-summary-description
  [{:keys [anchor-year five-year ten-year anchor-year-median
           five-year-median ten-year-median five-year-delta
           five-year-delta-pct ten-year-delta ten-year-delta-pct]}]
  [(if (pos? five-year-delta)
     (format
      "The total number of EHCPs in %d of %,.0f is expected to go up by %,.0f to %,.0f by the year %d, which is an increase of %,.1f%%."
      anchor-year anchor-year-median five-year-delta five-year-median five-year (* 100 five-year-delta-pct))
     (format
      "The total number of EHCPs in %d of %,.0f is expected to go down by %,.0f to %,.0f by the year %d, which is a decrease of %,.1f%%."
      anchor-year anchor-year-median (* -1 five-year-delta) five-year-median five-year (* -100 five-year-delta-pct)))
   (if (pos? ten-year-delta)
     (format "By %d it will have gone up by %,.0f to %,.0f, which is an increase of %,.1f%% over 10 years."
             ten-year ten-year-delta ten-year-median (* 100 ten-year-delta-pct))
     (format "By %d it will have gone down by %,.0f to %,.0f, which is a decrease of %,.1f%% over 10 years."
             ten-year (* -1 ten-year-delta) ten-year-median (* -100 ten-year-delta-pct)))])

(defn pct-ehcps-summary-map [pct-ehcps-summary {:keys [anchor-year]}]
  (let [anchor-year (or anchor-year (reduce min (:calendar-year pct-ehcps-summary)))
        five-year (+ 5 anchor-year)
        ten-year (+ 10 anchor-year)
        anchor-year-median (summary/value-at pct-ehcps-summary #(= anchor-year (:calendar-year %)) :median)
        five-year-median (summary/value-at pct-ehcps-summary #(= five-year (:calendar-year %)) :median)
        ten-year-median (summary/value-at pct-ehcps-summary #(= ten-year (:calendar-year %)) :median)
        five-year-delta (dfn/- five-year-median anchor-year-median)
        ten-year-delta (dfn/- ten-year-median anchor-year-median)]
    {:anchor-year anchor-year
     :five-year five-year
     :ten-year ten-year
     :anchor-year-median anchor-year-median
     :five-year-median five-year-median
     :ten-year-median ten-year-median
     :five-year-delta five-year-delta
     :ten-year-delta ten-year-delta}))

(defn pct-ehcps-summary-description
  [{:keys [anchor-year five-year ten-year
           anchor-year-median five-year-median ten-year-median
           five-year-delta ten-year-delta]}]
  [(if (pos? five-year-delta)
     (format
      "In %d EHCPs were issued for %,.1f%% of the total population. It is projcted to go up by %,.1f percentage points to %,.1f%% of the population by the year %d."
      anchor-year (* 100 anchor-year-median) (* 100 five-year-delta) (* 100 five-year-median) five-year)
     (format
      "In %d EHCPs were issued for %,.1f%% of the total population. It is projcted to go down by %,.1f percentage points to %,.1f%% of the population by the year %d."
      anchor-year (* 100 anchor-year-median) (* -100 five-year-delta) (* 100 five-year-median) five-year))
   (if (pos? ten-year-delta)
     (format "By %d it will have gone up by %,.1f%% to %,.1f percentage points over 10 years."
             ten-year (* 100 ten-year-delta) ten-year-median)
     (format "By %d it will have gone down by %,.1f%% to %,.1f percentage points over 10 years."
             ten-year (* -100 ten-year-delta) ten-year-median))])

(def base-chart-spec
  {:y   :median
   :irl :q1  :iru :q3  :ir-title "50% range"
   :orl :p05 :oru :p95 :or-title "90% range"})

(defn line-and-ribbon-and-rule-plot
  [{:keys [data group group-title colors-and-shapes
           x x-title
           y y-title y-format y-zero
           chart-title chart-width chart-height]
    :as chart-spec}]
  (vsl/line-and-ribbon-and-rule-plot
   (merge base-chart-spec chart-spec)))

#_
(transition-count-summary-description
 (transition-count-summary-map (-> summary :transition-count-summary :table) {}))

#_
(pct-ehcps-summary-description
 (pct-ehcps-summary-map (-> summary :pct-ehcps-summary :table) {}))

(defn summary-charts-and-data-from-config
  [{:keys [config-edn pqt-prefix
           anchor-year colors-and-shapes projection]}]
  (let [summary
        (summarise-from-config config-edn pqt-prefix)
        transition-count-plot
        (line-and-ribbon-and-rule-plot
         {:data              (-> summary
                                 :transition-count-summary
                                 :table
                                 (tc/add-column :projection projection)
                                 (tc/map-columns :calendar-year [:calendar-year] (fn [d] (str d))))
          :chart-title       "Total EHCPs"
          :chart-height      vs/full-height :chart-width vs/two-thirds-width
          :colors-and-shapes colors-and-shapes
          :x                 :calendar-year :x-title     "Census Year" :x-format "%b %Y"
          :y-title           "# EHCPs"      :y-zero      true          :y-scale  false
          :group             :projection    :group-title "Projection"})
        transition-count-summary-map
        (transition-count-summary-map
         (-> summary :transition-count-summary :table) {})
        transition-count-summary-description
        (transition-count-summary-description transition-count-summary-map)
        ehcp-pct-diff-summary-plot
        (line-and-ribbon-and-rule-plot
         {:data              (-> summary
                                 :ehcp-pct-diff-summary
                                 :table
                                 (tc/add-column :projection projection)
                                 (tc/map-columns :calendar-year [:calendar-year] (fn [d] (str d))))
          :chart-title       "% EHCP change YoY"
          :chart-height      vs/full-height      :chart-width vs/two-thirds-width
          :tooltip-formatf   (vsl/pct-summary-tooltip {:group :projection :x :calendar-year :tooltip-field :tooltip-column})
          :colors-and-shapes colors-and-shapes
          :x                 :calendar-year      :x-title     "Census Year" :x-format "%b %Y"
          :y-title           "% EHCP change YoY" :y-zero      false         :y-scale  false :y-format ".1%"
          :group             :projection         :group-title "Projection"})
        pct-ehcps-summary-plot
        (line-and-ribbon-and-rule-plot
         {:data              (-> summary
                                 :pct-ehcps-summary
                                 :table
                                 (tc/add-column :projection projection)
                                 (tc/map-columns :calendar-year [:calendar-year] (fn [d] (str d))))
          :chart-title       "EHCP % of 0-25 Population"
          :chart-height      vs/full-height         :chart-width vs/two-thirds-width
          :tooltip-formatf   (vsl/pct-summary-tooltip {:group :projection :x :calendar-year :tooltip-field :tooltip-column})
          :colors-and-shapes colors-and-shapes
          :x                 :calendar-year         :x-title     "Census Year" :x-format "%b %Y"
          :y-title           "EHCP % of 0-25 Population" :y-zero      true         :y-scale  false :y-format ".1%"
          :group             :projection            :group-title "Projection"})]
    (-> summary
        (assoc-in [:transition-count-summary :plot] transition-count-plot)
        (assoc-in [:transition-count-summary :summary-map] transition-count-summary-map)
        (assoc-in [:transition-count-summary :summary-description] transition-count-summary-description)

        (assoc-in [:ehcp-pct-diff-summary :plot] ehcp-pct-diff-summary-plot)

        (assoc-in [:pct-ehcps-summary :plot] pct-ehcps-summary-plot))))
