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

(defn population-by-year
  "Takes a census count and returns a t.m.dataset with a count of total
  population per year."
  [census-data]
  (-> census-data
      (tc/group-by [:simulation :calendar-year])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (summary/seven-number-summary [:calendar-year] :transition-count)
      (tc/order-by [:calendar-year])))

(defn year-counts [census-data file-name {:keys [color shape watermark value-key legend-label title base-chart-spec] :as _config
                                          :or {color colors/blue
                                               shape \A
                                               watermark ""
                                               value-key :transition-count
                                               base-chart-spec plot/base-pop-chart-spec
                                               legend-label "Population"
                                               title "Total EHCPs"}}]
  (let [counts (-> census-data
                   (tc/group-by [:simulation :calendar-year])
                   (tc/aggregate {value-key #(dfn/sum (value-key %))})
                   (summary/seven-number-summary [:calendar-year] value-key)
                   (tc/order-by [:calendar-year]))]
    (-> {:census-data counts}
        (merge {:color color :shape shape :legend-label legend-label :title title :watermark watermark :base-chart-spec base-chart-spec})
        (single-population/single-population-report)
        (vector)
        (large/create-workbook)
        (large/save-workbook! file-name))))
