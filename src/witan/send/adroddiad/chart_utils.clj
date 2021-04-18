(ns witan.send.adroddiad.chart-utils
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]))

(defn colors-and-shapes [census-data]
  (colors/colors-and-shapes census-data))

(defn ->large-charts [chart]
  (assoc chart ::large/images [{::large/image (-> chart ::plot/canvas :buffer plot/->byte-array)}]))
