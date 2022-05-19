(ns witan.send.adroddiad.transitions.output
  (:require [tablecloth.api :as tc]
            [witan.send.adroddiad.transitions :as t]))

(defn ->csv-with-ids [transitions out-file]
  (-> transitions
      (tc/select-columns [:id :calendar-year
                          :setting-1 :need-1 :academic-year-1
                          :setting-2 :need-2 :academic-year-2
                          :transition-type])
      (tc/order-by [:id :calendar-year])
      (tc/map-columns :transition-type [:setting-1 :setting-2] t/transition-type)
      (tc/write! out-file)))

(defn ->csv-model-ready [transitions out-file]
  (-> transitions
      (tc/select-columns [:calendar-year
                          :setting-1 :need-1 :academic-year-1
                          :setting-2 :need-2 :academic-year-2])
      (tc/write! out-file)))
