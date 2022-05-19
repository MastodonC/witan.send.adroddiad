(ns witan.send.adroddiad.chart-utils
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]))

(defn domains [census-data]
  {:needs          (into (sorted-set) (-> census-data :need))
   :settings       (into (sorted-set) (-> census-data :setting))
   :academic-years (let [ncys (into (sorted-set) (-> census-data :academic-year))]
                     (conj ncys
                           (dec (first ncys))
                           (inc (last ncys))))})

(defn colors-and-shapes
  ([census-data extra-domain-items palette]
   (colors/domain-colors-and-shapes
    (into []
          cat
          (conj (vals (domains census-data))
                extra-domain-items)) palette))
  ([census-data extra-domain-items]
   (colors-and-shapes extra-domain-items))
  ([census-data]
   (colors-and-shapes census-data [])))

(defn ->large-charts [chart]
  (assoc chart ::large/images [{::large/image (-> chart ::plot/canvas :buffer plot/->byte-array)}]))
