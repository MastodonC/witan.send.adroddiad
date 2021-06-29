(ns witan.send.adroddiad.single-population
  (:require [kixi.large :as large]
            [kixi.large.legacy :as ll]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [kixi.plot.colors :as colors]
            [witan.send.adroddiad.chart-utils :as chart-utils]))

(defn single-population-report [{:keys [census-data color shape legend-label title watermark]}]
  (-> {::series/series (series/ds->median-iqr-95-series census-data color shape)
       ::series/legend-spec [(series/legend-spec "Population" color (colors/legend-shape shape))]}
      (merge {::plot/legend-label legend-label
              ::plot/title {::plot/label title}}
             plot/base-pop-chart-spec
             {::large/data census-data
              ::large/sheet-name title})
      (plot/add-overview-legend-items)
      (plot/zero-y-index)
      (update ::plot/canvas plot/add-watermark watermark)
      (chart-utils/->large-charts)))
