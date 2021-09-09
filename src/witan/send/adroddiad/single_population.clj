(ns witan.send.adroddiad.single-population
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]
            [kixi.plot.series :as series]
            [witan.send.adroddiad.chart-utils :as chart-utils]))

(defn single-population-report [{:keys [census-data color shape legend-label title watermark base-chart-spec chartf]
                                 :or {base-chart-spec plot/base-pop-chart-spec
                                      chartf plot/zero-y-index}}]
  (-> {::series/series (series/ds->median-iqr-95-series census-data color shape)
       ::series/legend-spec [(series/legend-spec legend-label color (colors/legend-shape shape))]}
      (merge {::plot/legend-label legend-label
              ::plot/title {::plot/label title}}
             base-chart-spec
             {::large/data census-data
              ::large/sheet-name title})
      (plot/add-overview-legend-items)
      (chartf)
      (update ::plot/canvas plot/add-watermark watermark)
      (chart-utils/->large-charts)))
