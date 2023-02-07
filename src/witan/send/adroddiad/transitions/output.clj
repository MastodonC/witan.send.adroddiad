(ns witan.send.adroddiad.transitions.output
  (:require [tablecloth.api :as tc]
            [witan.send.adroddiad.transitions :as t]))

(def transition-column-names
  [:calendar-year
   :setting-1 :need-1 :academic-year-1
   :setting-2 :need-2 :academic-year-2])

(defn ->csv-with-ids
  ([transitions out-file]
   (->csv-with-ids transitions out-file []))
  ([transitions out-file additional-column-names]
   (-> transitions
       (tc/map-columns :transition-type [:setting-1 :setting-2] t/transition-type)
       (tc/select-columns ((comp distinct concat) [:id] transition-column-names [:transition-type] additional-column-names))
       (tc/order-by [:id :calendar-year])
       (tc/write! out-file))))

(defn ->csv-model-ready [transitions out-file]
  (-> transitions
      (tc/select-columns transition-column-names)
      (tc/write! out-file)))
