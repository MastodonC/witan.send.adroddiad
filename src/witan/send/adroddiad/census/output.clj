(ns witan.send.adroddiad.census.output
  (:require [tablecloth.api :as tc]))

(defn ->csv-with-ids [census out-file]
  (-> census
      (tc/select-columns [:id :calendar-year
                          :setting :need :academic-year])
      (tc/write! out-file)))
