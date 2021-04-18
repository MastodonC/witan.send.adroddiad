(ns witan.send.adroddiad.census
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.chart-utils :as chart-utils]))

(defn census-report [{:keys [census-data colors-and-shapes series-key legend-label report-sections]}]
  (let [data (-> census-data
                 (tc/group-by [:simulation :calendar-year series-key])
                 (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                 (summary/seven-number-summary [:calendar-year series-key] :transition-count)
                 (tc/order-by [:calendar-year series-key]))]
    (into []
          (map (fn [{:keys [title series]}]
                 (let [data-table (-> data
                                      (tc/select-rows #(series (series-key %)))
                                      (tc/order-by [series-key :calendar-year]))
                       grouped-data (-> data-table
                                        (tc/group-by [series-key]))]
                   (-> (series/grouped-ds->median-iqr-95-series-and-legend {::series/colors-and-shapes colors-and-shapes
                                                                            ::series/grouped-data grouped-data
                                                                            ::series/series-key series-key})
                       (merge {::plot/legend-label legend-label
                               ::plot/title {::plot/label title}}
                              plot/base-pop-chart-spec
                              {::large/data data-table
                               ::large/sheet-name title})
                       (plot/add-overview-legend-items)
                       (plot/zero-y-index)
                       (chart-utils/->large-charts)))))
          report-sections)))
