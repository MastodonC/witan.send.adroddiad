(ns witan.send.adroddiad.year-counts
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.gradient :as dt-grad]
            [witan.send.adroddiad.single-population :as single-population]
            [witan.send.adroddiad.summary :as summary]))

(defn population-by-year
  "Takes a census count and returns a t.m.dataset with a count of total
  population per year."
  [{:keys [census-data value-key]
    :or   {value-key :transition-count}}]
  (let [base-report (-> census-data
                        (tc/group-by [:simulation :calendar-year])
                        (tc/aggregate {value-key #(dfn/sum (value-key %))})
                        (summary/seven-number-summary [:calendar-year] value-key)
                        (tc/order-by [:calendar-year]))
        yoy-diffs   (-> base-report
                        (tc/order-by [:calendar-year])
                        (tc/add-columns {:median-yoy-diff   (fn add-year-on-year [ds]
                                                              (let [medians (:median ds)
                                                                    diffs   (dt-grad/diff1d medians)]
                                                                (into [] cat [[0] diffs])))
                                         :median-yoy-%-diff (fn add-year-on-year-% [ds]
                                                              (let [medians (:median ds)
                                                                    diffs   (dt-grad/diff1d medians)
                                                                    diff-%s (dfn// diffs (drop-last (:median ds)))]
                                                                (into [] cat [[0] diff-%s])))})
                        (tc/select-columns [:calendar-year :median-yoy-diff :median-yoy-%-diff]))]
    (-> base-report
        (tc/inner-join yoy-diffs [:calendar-year])
        (tc/select-columns [:calendar-year :min :low-95 :q1 :median :q3 :high-95 :max :total-median :pct-of-total :median-yoy-diff :median-yoy-%-diff])
        (tc/order-by [:calendar-year]))))

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
    {:file-name file-name
     :xl (-> {:census-data counts}
             (merge {:color color :shape shape :legend-label legend-label :title title :watermark watermark :base-chart-spec base-chart-spec :chartf chartf})
             (single-population/single-population-report)
             (vector)
             (large/create-workbook)
             (large/save-workbook! file-name))}))

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
