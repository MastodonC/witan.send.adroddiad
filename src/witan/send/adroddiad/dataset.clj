(ns witan.send.adroddiad.dataset
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.gradient :as gradient]
            [fastmath.core :as m]))

(defn add-diff-and-pct-diff [ds value-col order-col]
  (let [ds' (tc/order-by ds [order-col])
        values (get ds' value-col)
        diff (gradient/diff1d values)
        pct-change (sequence
                    (map (fn [d m] (m/approx (* 100 (double (/ d m))))))
                    diff values)]
    (-> ds'
        (tc/add-column :diff (into [nil] diff))
        (tc/add-column :pct-change (into [nil] pct-change)))))
