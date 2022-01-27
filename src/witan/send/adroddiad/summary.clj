(ns witan.send.adroddiad.summary
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset.reductions :as ds-reduce]))

(defn seven-number-summary [ds agg-columns value-column]
  (let [joined-ds (-> ds
                      (tc/join-columns :agg-column agg-columns {:result-type :seq})
                      (vector))]
    (-> (ds-reduce/group-by-column-agg
         :agg-column
         {:agg-column (ds-reduce/first-value :agg-column)
          :min        (ds-reduce/prob-quantile value-column 0.0)
          :low-95     (ds-reduce/prob-quantile value-column 0.05)
          :q1         (ds-reduce/prob-quantile value-column 0.25)
          :median     (ds-reduce/prob-quantile value-column 0.50)
          :q3         (ds-reduce/prob-quantile value-column 0.75)
          :high-95    (ds-reduce/prob-quantile value-column 0.95)
          :max        (ds-reduce/prob-quantile value-column 1.0)
          :row-count  (ds-reduce/row-count)}
         joined-ds)
        (tc/separate-column :agg-column agg-columns identity))))
