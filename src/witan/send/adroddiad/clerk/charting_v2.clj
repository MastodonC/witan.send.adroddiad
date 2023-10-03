(ns witan.send.adroddiad.clerk.charting-v2
  (:require
   [clojure2d.color :as color]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))


(defn color-and-shape-lookup [domain]
  (tc/dataset
   {:domain-value domain
    :color (cycle
            ;; I’d like to eventually do something based on these colours in v3
            ;; ["#fa814c" "#256cc6" "#fbe44c" "#50b938" "#59c4b8" "#29733c"]
            (into []
                  (map color/format-hex)
                  (color/palette :tableau-20)))
    :shape (cycle ["circle", "square", "cross", "diamond", "triangle-up", "triangle-down", "triangle-right", "triangle-left"])}))



(defn color-map [plot-data color-field color-lookup]
  (let [group-keys (into (sorted-set) (get plot-data color-field))
        filtered-colors (tc/select-rows color-lookup #(group-keys (get % :domain-value)))]
    {:field color-field
     :scale {:range (into [] (:color filtered-colors))
             :domain (into [] (:domain-value filtered-colors))}}))

(defn shape-map [plot-data shape-field shape-lookup]
  (let [group-keys (into (sorted-set) (get plot-data shape-field))
        filtered-shapes (tc/select-rows shape-lookup #(group-keys (get % :domain-value)))]
    {:field shape-field
     :scale {:range (into [] (:shape filtered-shapes))
             :domain (into [] (:domain-value filtered-shapes))}}))

(defn line-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           clerk-width
           x x-title x-format
           y y-title y-format y-zero
           group group-title
           colors-and-shapes]
    :or {chart-height 500
         chart-width 500
         clerk-width :full
         y-zero true}}]
  (let [tooltip [{:field group, :title group-title},
                 {:field x, :type "temporal", :format x-format, :title x-title},
                 {:field y, :title y-title ;; :format y-format
                  }]]
    (clerk/vl
     {::clerk/width clerk-width}
     {:height chart-height
      :width chart-width
      ;; :autosize {:type "fit" :contains "padding"}
      :title {:text chart-title
              :fontSize 24}
      :config {:legend {:titleFontSize 16
                        :labelFontSize 14}
               :axisX {:tickcount 7
                       :tickExtra true
                       :labelalign "center"
                       :titleFontSize 16
                       :labelFontSize 12}
               :axisY {:titleFontSize 16
                       :labelFontSize 12}}
      :data {:values (-> data
                         (tc/rows :as-maps))}
      :layer [{:mark {:type "line", :point {:filled false,
                                            :fill "white",
                                            :size 50
                                            :strokewidth 0.5}},
               :encoding {:y {:field y, :title y-title :type "quantitative" :scale {:zero y-zero}},
                          :x {:field x, :title x-title :format x-format :type "temporal"},
                          ;; color and shape scale and range must be specified or you get extra things in the legend
                          :color (assoc (color-map data group colors-and-shapes) :title group-title)
                          :shape (shape-map data group colors-and-shapes)
                          :tooltip tooltip}}]})))

(defn line-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           clerk-width
           x x-title x-format
           y y-title y-format y-zero
           irl iru ir-title
           orl oru or-title
           range-format-f
           group group-title
           colors-and-shapes]
    :or {chart-height 640
         chart-width 824
         clerk-width :full
         range-format-f (fn [lower upper]
                          (format "%,f - %,1f" lower upper))
         y-zero true}}]
  (let [tooltip [{:field group, :title group-title},
                 {:field x, :type "temporal", :format x-format, :title x-title},
                 {:field y, :title y-title ;; :format y-format
                  }
                 {:field :ir :title ir-title}
                 {:field :or :title or-title}]]
    (clerk/vl
     {::clerk/width clerk-width}
     {:height chart-height
      :width chart-width
      ;; :autosize {:type "fit" :contains "padding"}
      :title {:text chart-title
              :fontSize 24}
      :config {:legend {:titleFontSize 16
                        :labelFontSize 14}
               :axisX {:tickcount 7
                       :tickExtra true
                       :labelalign "center"
                       :titleFontSize 16
                       :labelFontSize 12}
               :axisY {:titleFontSize 16
                       :labelFontSize 12}}
      :data {:values (-> data
                         (tc/map-columns :ir [irl iru] range-format-f)
                         (tc/map-columns :or [orl oru] range-format-f)
                         (tc/rows :as-maps))}
      :layer [{:mark "errorband"
               :encoding {:y {:field iru :title y-title :type "quantitative"}
                          :y2 {:field irl}
                          :x {:field x :title x-title :format x-format :type "temporal"}
                          :color {:field group :title group-title}
                          :tooltip tooltip}}
              {:mark "errorband"
               :encoding {:y {:field oru :title y-title :type "quantitative"}
                          :y2 {:field orl}
                          :x {:field x :title x-title :format x-format :type "temporal"}
                          :color {:field group :title group-title}
                          :tooltip tooltip}}
              {:mark {:type "line", :point {:filled false,
                                            :fill "white",
                                            :size 50
                                            :strokewidth 0.5}},
               :encoding {:y {:field y, :title y-title :type "quantitative" :scale {:zero y-zero}},
                          :x {:field x, :title x-title :format x-format :type "temporal"},
                          ;; color and shape scale and range must be specified or you get extra things in the legend
                          :color (color-map data group colors-and-shapes)
                          :shape (shape-map data group colors-and-shapes)
                          :tooltip tooltip}}]})))

(defn heatmap-desc
  "Creates a map that is a description of a heatmap to pass to clerk/vl"
  [{:keys [title
           x-field x-sort-field x-field-label x-field-desc
           y-field y-sort-field y-field-label y-field-desc
           color-field white-text-test data height width
           color-scheme color-type color-domain]
    :or {height 100
         width 550
         white-text-test "datum['% change'] > 10 || datum['% change'] < -10"
         color-scheme "viridis"
         color-type "gradient"}}]
  (let [tooltip [{:field y-field-desc :type "ordinal" :title y-field-label}
                 {:field x-field-desc :type "ordinal" :title x-field-label}
                 {:field color-field :type "quantitative"}]]
    {:data {:values data}
     :encoding {:x {:field (or x-field-desc x-field) :type "ordinal" :sort (or x-sort-field x-field) :title (or x-field-label x-field)}
                :y {:field (or y-field-desc y-field) :type "ordinal" :sort (or y-sort-field y-field) :title (or y-field-label y-field)}}
     :config {:axis {:grid true
                     :tickBand "extent"
                     :titleFontSize 16
                     :labelFontSize 12}}
     :title {:text title
             :fontSize 24}
     :height height
     :width width
     :layer [{:encoding {:color {:field color-field
                                 :legend {:gradientLength 200}
                                 :scale {:domainMid 0
                                         :scheme "blueorange"}
                                 :title color-field
                                 :type "quantitative"}
                         :tooltip tooltip}
              :mark "rect"}
             {:encoding {:color {:condition {:test white-text-test
                                             :value "white"}
                                 :value "black"}
                         :text {:field color-field :type "quantitative"}
                         :tooltip tooltip}
              :mark "text"}]}))
