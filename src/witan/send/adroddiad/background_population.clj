(ns witan.send.adroddiad.background-population
  (:require [com.climate.claypoole.lazy :as lazy]
            [kixi.large :as xl]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]))

(def early-years (into (sorted-set) (range -5 0)))
(def primary #{0 1 2 3 4 5 6})
(def secondary #{7 8 9 10 11})
(def post-16 #{12 13 14})
(def post-19 #{15 16 17 18 19 20})

(def chart-sections
  [{:title "Early Years" :ncys early-years}
   {:title "Primary" :ncys primary}
   {:title "Secondary" :ncys secondary}
   {:title "Post 16" :ncys post-16}
   {:title "Post 19" :ncys post-19}])

(defn friendly-column-names [ds]
  (tc/rename-columns ds
                     {:calendar-year "Calendar Year"
                      :academic-year "NCY"
                      :population    "Population"}))

;; FIXME: This should return the chart and the data for the table and
;; any formatting things so that I can pass it to a function to create
;; a xl
(defn chart [chart-data {:keys [legend-label watermark title ncys]}]
  (let [population-subset (select-keys chart-data ncys)
        lines (into []
                    (map (fn [[_ v]]
                           (:series v)))
                    population-subset)
        legends (into []
                      (map (fn [[_ v]]
                             (:legend v)))
                      population-subset)]
    (assoc chart-data
           :chart
           (-> {::series/series lines
                ::series/legend-spec legends
                ::plot/legend-label legend-label
                ::plot/title title}
               plot/zero-y-index
               (update ::plot/canvas plot/add-watermark watermark)))))

(defn format-table [ds]
  (tc/reorder-columns ds [:calendar-year :academic-year :population]))


(defn ->population [file-name]
  (-> (tc/dataset file-name {:key-fn keyword})
      (tc/group-by [:academic-year] {:result-type :as-map})))

(comment

  (def population (->population "./resources/population.csv"))

  (def colors-and-shapes
    {-5 {:color [247.0, 182.0, 210.0, 255.0], :shape \-, :legend-shape \-}
     -4 {:color [227.0, 119.0, 194.0, 255.0], :shape \V, :legend-shape \A}
     -3 {:color [196.0, 156.0, 148.0, 255.0], :shape \\, :legend-shape \/}
     -2 {:color [140.0, 86.0, 75.0, 255.0], :shape \^, :legend-shape \v}
     -1 {:color [127.0, 127.0, 127.0, 255.0], :shape \|, :legend-shape \|}
     0 {:color [219.0, 219.0, 141.0, 255.0], :shape \O, :legend-shape \O}
     1 {:color [199.0, 199.0, 199.0, 255.0], :shape \/, :legend-shape \\}
     2 {:color [188.0, 189.0, 34.0, 255.0], :shape \o, :legend-shape \o}
     3 {:color [158.0, 218.0, 229.0, 255.0], :shape \A, :legend-shape \V}
     4 {:color [23.0, 190.0, 207.0, 255.0], :shape \>, :legend-shape \>}
     5 {:color [152.0, 223.0, 138.0, 255.0], :shape \x, :legend-shape \x}
     6 {:color [255.0, 187.0, 120.0, 255.0], :shape \v, :legend-shape \^}
     7 {:color [255.0, 127.0, 14.0, 255.0], :shape \S, :legend-shape \S}
     8 {:color [174.0, 199.0, 232.0, 255.0], :shape \{, :legend-shape \{}
     9 {:color [44.0, 160.0, 44.0, 255.0], :shape \s, :legend-shape \s}
     10 {:color [31.0, 119.0, 180.0, 255.0], :shape \<, :legend-shape \<}
     11 {:color [255.0, 152.0, 150.0, 255.0], :shape \}, :legend-shape \}}
     12 {:color [214.0, 39.0, 40.0, 255.0], :shape \-, :legend-shape \-}
     13 {:color [197.0, 176.0, 213.0, 255.0], :shape \V, :legend-shape \A}
     14 {:color [148.0, 103.0, 189.0, 255.0], :shape \\, :legend-shape \/}
     15 {:color [247.0, 182.0, 210.0, 255.0], :shape \^, :legend-shape \v}
     16 {:color [227.0, 119.0, 194.0, 255.0], :shape \|, :legend-shape \|}
     17 {:color [196.0, 156.0, 148.0, 255.0], :shape \O, :legend-shape \O}
     18 {:color [140.0, 86.0, 75.0, 255.0], :shape \/, :legend-shape \\}
     19 {:color [127.0, 127.0, 127.0, 255.0], :shape \o, :legend-shape \o}
     20 {:color [219.0, 219.0, 141.0, 255.0], :shape \A, :legend-shape \V}})

  (def data
    (into (sorted-map)
          (comp
           (map (fn [[k v]]
                  [(:academic-year k) {:data v}]))
           (map (fn [[academic-year {:keys [data] :as m}]]
                  [academic-year (assoc m :series
                                        (series/line-series
                                         {:ds data
                                          :x :calendar-year
                                          :y :population
                                          :color (-> academic-year colors-and-shapes :color)
                                          :shape (-> academic-year colors-and-shapes :shape)}))]))
           (map (fn fn [[academic-year {:keys [data] :as m}]]
                  [academic-year (assoc m :legend
                                        (series/legend-spec
                                         academic-year
                                         (-> academic-year colors-and-shapes :color)
                                         (-> academic-year colors-and-shapes :legend-shape)))])))
          population))


  (def charts
    (into []
          (comp
           (map (fn [m] (assoc m
                              :watermark "Test"
                              :legend-label "NCYs")))
           (map (partial chart data)))
          chart-sections))



  )
