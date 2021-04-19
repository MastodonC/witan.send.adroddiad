(ns witan.send.adroddiad.year-counts
  (:require [kixi.large :as large]
            [kixi.large.legacy :as ll]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [kixi.plot.colors :as colors]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.single-population :as single-population]
            [witan.send.adroddiad.summary :as summary]))

(defn year-counts [census-data file-name]
  (let [counts (-> census-data
                   (tc/group-by [:simulation :calendar-year])
                   (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                   (summary/seven-number-summary [:calendar-year] :transition-count)
                   (tc/order-by [:calendar-year]))]
    (-> {:census-data counts}
        (merge {:color colors/blue :shape \A :legend-label "Population" :title "Total EHCPs"})
        (single-population/single-population-report)
        (vector)
        (large/create-workbook)
        (large/save-workbook! file-name))))
