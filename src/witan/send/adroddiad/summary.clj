(ns witan.send.adroddiad.summary
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset.reductions :as ds-reduce]))

(defn seven-number-summary [data agg-columns value-column]
  (ds-reduce/group-by-column-agg
   agg-columns
   {;;:agg-column (ds-reduce/first-value :agg-column)
    :min        (ds-reduce/prob-quantile value-column 0.0)
    :low-95     (ds-reduce/prob-quantile value-column 0.05)
    :q1         (ds-reduce/prob-quantile value-column 0.25)
    :median     (ds-reduce/prob-quantile value-column 0.50)
    :q3         (ds-reduce/prob-quantile value-column 0.75)
    :high-95    (ds-reduce/prob-quantile value-column 0.95)
    :max        (ds-reduce/prob-quantile value-column 1.0)
    :row-count  (ds-reduce/row-count)}
   (if ((some-fn vector? seq?) data) data (vector data))))

(defn default-summariser [value-column]
  {:min        (ds-reduce/prob-quantile value-column 0.0)
   :low-95     (ds-reduce/prob-quantile value-column 0.05)
   :q1         (ds-reduce/prob-quantile value-column 0.25)
   :median     (ds-reduce/prob-quantile value-column 0.50)
   :mean       (ds-reduce/mean value-column)
   :q3         (ds-reduce/prob-quantile value-column 0.75)
   :high-95    (ds-reduce/prob-quantile value-column 0.95)
   :max        (ds-reduce/prob-quantile value-column 1.0)
   :row-count  (ds-reduce/row-count)})

(defn summary-statistics [data agg-columns summariser]
  (ds-reduce/group-by-column-agg
   agg-columns
   summariser
   (if ((some-fn vector? seq?) data) data (vector data))))
