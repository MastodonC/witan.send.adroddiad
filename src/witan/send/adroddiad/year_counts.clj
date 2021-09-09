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
  [{:keys [census-data value-key]
    :or {value-key :transition-count}}]
  (-> census-data
      (tc/group-by [:simulation :calendar-year])
      (tc/aggregate {value-key #(dfn/sum (value-key %))})
      (summary/seven-number-summary [:calendar-year] value-key)
      (tc/order-by [:calendar-year])))

(defn year-counts [census-data file-name {:keys [color shape watermark value-key legend-label title base-chart-spec chartf] :as _config
                                          :or {color colors/blue
                                               shape \A
                                               watermark ""
                                               value-key :transition-count
                                               base-chart-spec plot/base-pop-chart-spec
                                               legend-label "Population"
                                               chartf plot/zero-y-index
                                               title "Total EHCPs"}}]
  (let [counts (population-by-year {:census-data census-data
                                    :value-key value-key})]
    (-> {:census-data counts}
        (merge {:color color :shape shape :legend-label legend-label :title title :watermark watermark :base-chart-spec base-chart-spec :chartf chartf})
        (single-population/single-population-report)
        (vector)
        (large/create-workbook)
        (large/save-workbook! file-name))))

(defn population-by-year-chart
  [{:keys [census-data population-by-year-data file-name color shape watermark value-key legend-label title base-chart-spec chartf] :as _config
    :or {color colors/blue
         shape \A
         watermark ""
         value-key :transition-count
         base-chart-spec plot/base-pop-chart-spec
         legend-label "Population"
         chartf plot/zero-y-index
         title "Total EHCPs"
         file-name "population-by-year.xlsx"}}]
  (let [counts (if census-data
                 (population-by-year {:census-data census-data
                                      :value-key value-key})
                 population-by-year-data)]
    (-> {:census-data counts}
        (merge {:color color :shape shape :legend-label legend-label :title title :watermark watermark :base-chart-spec base-chart-spec :chartf chartf})
        (single-population/single-population-report)
        (vector)
        (large/create-workbook)
        (large/save-workbook! file-name))))
