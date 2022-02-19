(ns witan.send.adroddiad.single-population
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]
            [kixi.plot.series :as series]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [tablecloth.api :as tc]))

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

(defn two-populations-report [{:keys [first-census second-census colors shapes legend-label title watermark base-chart-spec chartf]
                               :or {base-chart-spec plot/base-pop-chart-spec
                                    chartf plot/zero-y-index}}]
  (-> {::series/series (into []
                             (mapcat (fn [[ds col]] (series/ds->median-iqr-95-series ds col (nth shapes 0))))
                             [[first-census (nth colors 0)] [second-census (nth colors 1)]])
       ::series/legend-spec [(series/legend-spec legend-label (nth colors 0) (colors/legend-shape (nth shapes 0)))]
       #_(into []
               (mapcat (fn [[ds col s]] (series/legend-spec ds col (colors/legend-shape s))))
               [["Scenario Population" (nth colors 0) (nth shapes 0)] ["Baseline Population" (nth colors 1) (nth shapes 1)]])}
      (merge {::plot/legend-label legend-label
              ::plot/title {::plot/label title}}
             base-chart-spec
             {::large/data (tc/append first-census second-census)
              ::large/sheet-name title})
      (plot/add-overview-legend-items)
      (chartf)
      (update ::plot/canvas plot/add-watermark watermark)
      (chart-utils/->large-charts)))
