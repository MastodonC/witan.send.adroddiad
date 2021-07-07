(ns witan.send.adroddiad.census
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.chart-utils :as chart-utils]))

(defn census-domain [census]
  (let [ays (into (sorted-set) (-> census :academic-year))
        needs (into (sorted-set) (-> census :need))
        settings (into (sorted-set) (-> census :setting))]
    (into []
          cat
          [ays needs settings])))

(defn census-report [{:keys [census-data colors-and-shapes series-key legend-label report-sections file-name watermark base-chart-spec value-key]
                      :or {watermark ""
                           base-chart-spec plot/base-pop-chart-spec
                           value-key :transition-count}}]
  (println (str "Building " file-name))
  (let [data (-> census-data
                 (tc/group-by [:simulation :calendar-year series-key])
                 (tc/aggregate {value-key #(dfn/sum (value-key %))})
                 (summary/seven-number-summary [:calendar-year series-key] value-key)
                 (tc/order-by [:calendar-year series-key]))]
    (try (-> (into []
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
                                       base-chart-spec
                                       {::large/data data-table
                                        ::large/sheet-name title})
                                (plot/add-overview-legend-items)
                                (plot/zero-y-index)
                                (update ::plot/canvas plot/add-watermark watermark)
                                (chart-utils/->large-charts)))))
                   report-sections)
             (large/create-workbook)
             (large/save-workbook! file-name))
         (println (str file-name " complete!"))
         {:file-name file-name}
         (catch Exception e (println "Failed to create " file-name)
                (ex-info (str "Failed to create " file-name) {:file-name file-name} e)))))
