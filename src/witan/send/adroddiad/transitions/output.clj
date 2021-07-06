(ns witan.send.adroddiad.transitions.output
  (:require [tablecloth.api :as tc]))

(defn ->csv-with-ids [transitions out-file]
  (-> transitions
      (tc/select-columns [:id :calendar-year
                          :setting-1 :need-1 :academic-year-1
                          :setting-2 :need-2 :academic-year-2])
      (tc/write! out-file)))

(defn ->csv-model-ready [transitions out-file]
  (-> transitions
      (tc/select-columns [:calendar-year
                          :setting-1 :need-1 :academic-year-1
                          :setting-2 :need-2 :academic-year-2])
      (tc/write! out-file)))
