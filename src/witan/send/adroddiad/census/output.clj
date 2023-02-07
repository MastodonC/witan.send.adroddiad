(ns witan.send.adroddiad.census.output
  (:require [tablecloth.api :as tc]))

(def census-column-names
  [:id :calendar-year
   :setting :need :academic-year])

(defn census-columns [census]
  (tc/select-columns census census-column-names))

(defn ->csv-with-ids
  ([census out-file]
   (->csv-with-ids census out-file []))
  ([census out-file additional-column-names]
   (-> census
       (tc/select-columns ((comp distinct concat) census-column-names additional-column-names))
       (tc/order-by [:id :calendar-year])
       (tc/write! out-file))))
