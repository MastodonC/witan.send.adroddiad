(ns witan.send.adroddiad.chart-utils
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]))

(defn colors-and-shapes [census-data]
  (colors/colors-and-shapes census-data))

(defn domains [census-data]
  {:needs          (into (sorted-set) (-> census-data :need))
   :settings       (into (sorted-set) (-> census-data :setting))
   :academic-years (into (sorted-set) (-> census-data :academic-year))})

(defn ->large-charts [chart]
  (assoc chart ::large/images [{::large/image (-> chart ::plot/canvas :buffer plot/->byte-array)}]))
