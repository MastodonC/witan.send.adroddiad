(ns witan.send.adroddiad.census.output
  (:require [tablecloth.api :as tc]))

(defn census-columns [census]
  (tc/select-columns
   census
   [:id :calendar-year
    :setting :need :academic-year]))

(defn ->csv-with-ids [census out-file]
  (-> census
      census-columns
      (tc/write! out-file)))
