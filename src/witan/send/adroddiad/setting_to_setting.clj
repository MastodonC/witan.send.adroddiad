(ns witan.send.adroddiad.setting-to-setting
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.reductions :as ds-reduce]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.transitions :as tr]))


(defn setting-to-setting [simulation-results-seq]
  (let [agg-columns [:academic-year-1 :academic-year-2 :calendar-year :setting-1 :setting-2 :simulation]
        joined-ds-seq (sequence
                       (map (fn [ds] (-> ds
                                         (tc/drop-columns [:need-1 :need-2])
                                         (tc/join-columns :agg-column agg-columns {:result-type :seq})
                                         ;; (tc/clone) ;; "Elapsed time: 293795.505423 msecs"
                                         ;; "Elapsed time: 251436.885959 msecs" w/o clone
                                         )))
                       simulation-results-seq)]
    (-> (->> (-> (ds-reduce/group-by-column-agg
                  :agg-column
                  {:agg-column (ds-reduce/first-value :agg-column)
                   :transition-count (ds-reduce/sum :transition-count)}
                  joined-ds-seq)
                 (tc/map-columns :agg-column [:agg-column] butlast)
                 (vector))
             (ds-reduce/group-by-column-agg
              :agg-column
              {:agg-column (ds-reduce/first-value :agg-column)
               :min        (ds-reduce/prob-quantile :transition-count 0.0)
               :low-95     (ds-reduce/prob-quantile :transition-count 0.05)
               :q1         (ds-reduce/prob-quantile :transition-count 0.25)
               :median     (ds-reduce/prob-quantile :transition-count 0.50)
               :q3         (ds-reduce/prob-quantile :transition-count 0.75)
               :high-95    (ds-reduce/prob-quantile :transition-count 0.95)
               :max        (ds-reduce/prob-quantile :transition-count 1.0)}))
        (tc/separate-column :agg-column (butlast agg-columns) identity)
        (tc/drop-columns [:min :max])
        (tc/order-by [:setting-1 :setting-2 :calendar-year :academic-year-1 :academic-year-2])
        (tc/rename-columns
         (zipmap [:setting-1 :setting-2 :calendar-year :academic-year-1 :academic-year-2 :low-95 :q1 :median :q3 :high-95]
                 ["Setting 1" "Setting 2" "Calendar Year" "Academic Year 1" "Academic Year 2" "Low 95%" "Q1" "Median" "Q3" "High 95%"]))
        (tc/select-columns ["Setting 1" "Setting 2" "Calendar Year" "Academic Year 1" "Academic Year 2" "Low 95%" "Q1" "Median" "Q3" "High 95%"]))))
