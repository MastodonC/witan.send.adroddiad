(ns witan.send.adroddiad.setting-to-setting
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.transitions :as tr]))


(defn setting-to-setting [simulation-results]
  (-> simulation-results
      (tc/group-by [:calendar-year :setting-1 :setting-2 :academic-year-1 :academic-year-2])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (summary/seven-number-summary [:calendar-year :setting-1 :setting-2 :academic-year-1 :academic-year-2] :transition-count)
      (tc/drop-columns [:min :max])
      (tc/order-by [:calendar-year :setting-1 :setting-2 :academic-year-1 :academic-year-2])
      (tc/rename-columns
       (zipmap [:setting-1 :setting-2 :calendar-year :academic-year-1 :academic-year-2 :low-95 :q1 :median :q3 :high-95]
               ["Setting 1" "Setting 2" "Calendar Year" "Academic Year 1" "Academic Year 2" "Low 95%" "Q1" "Median" "Q3" "High 95%"]))
      (tc/select-columns ["Setting 1" "Setting 2" "Calendar Year" "Academic Year 1" "Academic Year 2" "Low 95%" "Q1" "Median" "Q3" "High 95%"])))
