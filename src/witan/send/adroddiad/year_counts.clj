(ns witan.send.adroddiad.year-counts
  )

(defn year-counts [census-data]
  (let [counts (-> census-data
                   (tc/group-by [:simulation :calendar-year])
                   (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                   (seven-number-summary [:calendar-year] :transition-count)
                   (tc/order-by [:calendar-year]))])
  (-> {:census-data counts}
      (merge {:color colors/blue :shape \A :legend-label "Population" :title "Total EHCPs"})
      (single-population-report)
      (vector)
      (large/create-workbook)
      (large/save-workbook! "workpackage-1-1/ehcp-population.xlsx")))
