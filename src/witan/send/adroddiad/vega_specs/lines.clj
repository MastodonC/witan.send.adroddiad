(ns witan.send.adroddiad.vega-specs.lines
  (:require
   [clojure.string :as str]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.vega-specs :as vs]))

(def base-chart-spec
  {:y   :median
   :irl :q1  :iru :q3
   :orl :p05 :oru :p95})

(defn five-number-summary-string
  "Format five number summaries [`orl` `irl` `y` `iru` `oru`] into a single string
   of the form \"y (orl (irl↔iru) oru)\" 
   using format `fmt` for each summary (default \"%,.0f\")
   after applying (optional) function `f` to it.
   (So for percentages specify `:fmt \"%.0f%%\" :f (partial * 100)`.)"
  [[orl irl y iru oru] & {:keys [fmt f]
                          :or   {fmt "%,.0f"
                                 f   identity}}]
  (apply format
         (apply format "%s (%s (%s↔%s) %s)" (repeat 5 fmt))
         (map f [y orl irl iru oru])))

(comment
  (five-number-summary-string ["p05" "q1" "median" "q3" "p95"] :fmt "%s")
  ;; => "median (p05 (q1↔q3) p95)"
  (five-number-summary-string [0.3 0.1 0.2 0.4 0.5] :fmt "%.0f%%" :f (partial * 100))
  ;; => "20% (30% (10%↔40%) 50%)"
  
  )

(defn number-summary-tooltip
  [{:keys [tooltip-field group x order-field decimal-places]
    :or   {tooltip-field  :tooltip-field
           x              :analysis-date
           decimal-places 0}}]
  (let [dp-string (clojure.string/replace "%,.0f (%,.0f (%,.0f↔%,.0f) %,.0f)"
                                          "0" (str decimal-places))]
    (fn [ds]
      (-> ds
          (tc/map-columns
           tooltip-field [:p05 :q1 :median :q3 :p95]
           (fn [p05 q1 median q3 p95] (format
                                       dp-string
                                       median p05 q1 q3 p95)))
          (tc/order-by [(or order-field x)])
          (tc/select-columns [group x tooltip-field])
          (tc/pivot->wider [group] [tooltip-field] {:drop-missing? false})
          (tc/replace-missing :all :value "")
          (tc/rows :as-maps)))))

(defn pct-summary-tooltip
  [{:keys [tooltip-field group x order-field]
    :or {tooltip-field :tooltip-field
         x :analysis-date}}]
  (fn [ds]
    (-> ds
        (tc/map-columns
         tooltip-field [:p05 :q1 :median :q3 :p95]
         (fn [p05 q1 median q3 p95]
           (format "%.2f%% (%.2f%% (%.2f%%↔%.2f%%) %.2f%%)"
                   (* 100 median) (* 100 p05) (* 100 q1) (* 100 q3) (* 100 p95))))
        (tc/order-by [(or order-field x)])
        (tc/select-columns [group x tooltip-field])
        (tc/pivot->wider [group] [tooltip-field] {:drop-missing? false})
        (tc/replace-missing :all :value "")
        (tc/rows :as-maps))))

(defn line-and-ribbon-and-rule-plot
  [{:keys [data
           chart-title
           x x-title x-format x-scale
           y y-title y-format y-scale y-zero
           orl irl iru oru
           tooltip-field tooltip-formatf
           group group-title order-field
           chart-height chart-width
           colors-and-shapes]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-domain      false
           y-zero        true
           y-format      ",.0f"
           tooltip-field :tooltip-column}}]
  (let [;; TODO: Find a way to get legend shape/colour into here
        tooltip-formatf (or tooltip-formatf
                            (number-summary-tooltip {:tooltip-field tooltip-field
                                                     :group         group
                                                     :order-field   order-field
                                                     :x             x}))]
    {:data     {:values (-> data
                            (tc/rows :as-maps))}
     :height   chart-height
     :width    chart-width
     :title    {:text chart-title :fontSize 24}
     :config   {:legend {:titleFontSize 20 :labelFontSize 14 :labelLimit 0}
                :axisX  {:titleFontSize 16 :labelFontSize 12}
                :axisY  {:titleFontSize 16 :labelFontSize 12}}
     :encoding {:x {:field x :title x-title :type "temporal"
                    :scale {:domain x-scale :x-xero false}
                    ;; :axis {:format ["%Y"] :tickCount {:interval "month" :step 12}}
                    }}
     :layer    [{:encoding {:color (vs/color-map data group colors-and-shapes)
                            :shape (vs/shape-map data group colors-and-shapes)
                            :y     {:field y
                                    :type  "quantitative"
                                    :axis  {:format y-format}
                                    :scale {:domain y-scale :zero y-zero}}}
                 :layer    [{:mark {:type  "line"
                                    :size  5
                                    :point {:filled      true #_false
                                            :size        150
                                            :strokewidth 0.5}}}
                            {:mark     "errorband"
                             :encoding {:y     {:field oru :title y-title :type "quantitative"}
                                        :y2    {:field orl}
                                        :color {:field group :title group-title}}}
                            {:mark     "errorband"
                             :encoding {:y     {:field iru :title y-title :type "quantitative"}
                                        :y2    {:field irl}
                                        :color {:field group :title group-title}}}
                            {:transform [{:filter {:param "hover" :empty false}}] :mark {:type "point" :size 200 :strokeWidth 5}}]}
                {:data     {:values (tooltip-formatf data)}
                 :mark     {:type "rule" :strokeWidth 4}
                 :encoding {:opacity {:condition {:value 0.3 :param "hover" :empty false}
                                      :value     0}
                            :tooltip (into [{:field x :type "temporal" :format x-format :title x-title}]
                                           (map (fn [g] {:field g}))
                                           (if order-field
                                             (-> data
                                                 (tc/order-by [order-field])
                                                 (get group))
                                             (into (sorted-set) (data group))))}
                 :params   [{:name   "hover"
                             :select {:type    "point"
                                      :size    200
                                      :fields  [x]
                                      :nearest true
                                      :on      "pointerover"
                                      :clear   "pointerout"}}]}]}))

(defn line-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-zero y-scale y-tooltip
           oru irl iru orl
           group group-title
           colors-and-shapes]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           y-format     ",.0f"
           y-zero       true
           y-scale      false
           y-tooltip    :y-tooltip}}]
  (let [add-y-tooltip-col-f (fn [ds] (tc/map-columns ds y-tooltip [orl irl y iru oru]
                                                     #(five-number-summary-string %& {:fmt (str "%" y-format)})))
        tooltip             [{:field group :title group-title}
                             {:field x :title x-title :type "temporal" :format x-format }
                             {:field y-tooltip :title y-title}
                             {:field y :title y-title}]]
    {:height   chart-height
     :width    chart-width
     :title    {:text     chart-title
                :fontSize 24}
     :config   {:legend {:titleFontSize 20
                         :labelFontSize 14
                         :labelLimit    0}
                :axisX  {:titleFontSize 16
                         :labelFontSize 12}
                :axisY  {:titleFontSize 16
                         :labelFontSize 12}}
     :data     {:values (-> data
                            add-y-tooltip-col-f
                            (tc/rows :as-maps))}
     :encoding {:x {:field  x
                    :title  x-title
                    :type   "temporal"
                    :format x-format
                    :axis   {:format x-format}}
                :y {:title y-title
                    :type  "quantitative"
                    :scale {:domain y-scale
                            :zero   y-zero}}}
     :layer    [{:mark     {:type "line"
                            :size 5}
                 :encoding {:y       {:field y}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field oru}
                            :y2      {:field orl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}]}))

(defn line-shape-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-zero y-scale y-tooltip
           oru irl iru orl
           group group-title
           colors-and-shapes]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           y-format     ",.0f"
           y-zero       true
           y-scale      false
           y-tooltip    :y-tooltip}}]
  (let [add-y-tooltip-col-f (fn [ds] (tc/map-columns ds y-tooltip [orl irl y iru oru]
                                                     #(five-number-summary-string %& {:fmt (str "%" y-format)})))
        tooltip             [{:field group :title group-title}
                             {:field x :title x-title :type "temporal" :format x-format }
                             {:field y-tooltip :title y-title}
                             {:field y :title y-title}]]
    {:height   chart-height
     :width    chart-width
     :title    {:text     chart-title
                :fontSize 24}
     :config   {:legend {:titleFontSize 20
                         :labelFontSize 14
                         :labelLimit    0}
                :axisX  {:titleFontSize 16
                         :labelFontSize 12}
                :axisY  {:titleFontSize 16
                         :labelFontSize 12}}
     :data     {:values (-> data
                            add-y-tooltip-col-f
                            (tc/rows :as-maps))}
     :encoding {:x {:field  x
                    :title  x-title
                    :type   "temporal"
                    :format x-format
                    :axis   {:format x-format}}
                :y {:title y-title
                    :type  "quantitative"
                    :scale {:domain y-scale
                            :zero   y-zero}}}
     :layer    [{:mark     {:type  "line"
                            :size  2
                            :point {:filled      false
                                    :fill        "white"
                                    :size        50
                                    :strokewidth 0.5}}
                 :encoding {:y       {:field y}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :shape   (vs/shape-map data group colors-and-shapes)
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field oru}
                            :y2      {:field orl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}]}))

(defn line-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-zero y-scale
           group group-title
           colors-and-shapes
           legend]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           #_#_y-format ".0f"
           y-zero       true
           y-scale      false
           legend       true}}]
  (let [tooltip [{:field group :title group-title}
                 {:field x :title x-title :format x-format :type "temporal"}
                 {:field y :title y-title #_#_:format y-format}]]
    {:height   chart-height
     :width    chart-width
     :title    {:text     chart-title
                :fontSize 24}
     :config   {:legend {:titleFontSize 20
                         :labelFontSize 14
                         :labelLimit    0}
                :axisX  {:tickcount     7
                         :tickExtra     true
                         :labelalign    "center"
                         :titleFontSize 16
                         :labelFontSize 12}
                :axisY  {:titleFontSize 16
                         :labelFontSize 12}}
     :data     {:values (-> data
                            (tc/rows :as-maps))}
     :encoding {:x     {:field  x
                        :title  x-title
                        :type   "temporal"
                        :format x-format
                        :axis   {:format x-format}}
                :y     {:title y-title
                        :type  "quantitative"
                        :scale {:domain y-scale
                                :zero   y-zero}}
                :color {:legend legend}}
     :layer    [{:mark     {:type  "line"
                            :size  2
                            :point {:filled      false
                                    :fill        "white"
                                    :size        50
                                    :strokewidth 0.5}}
                 :encoding {:y       {:field y}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (assoc (vs/color-map data group colors-and-shapes) :title group-title)
                            :shape   (vs/shape-map data group colors-and-shapes)
                            :tooltip tooltip}}]}))

(defn remove-tooltips [chart]
  (assoc chart :layer
         [(-> chart
              :layer
              second
              (dissoc :encoding)
              (assoc-in [:mark :strokeWidth] 0))
          (-> chart
              :layer
              first
              (assoc :layer
                     (remove #(contains? % :transform) (-> chart
                                                           :layer
                                                           first
                                                           :layer))))]))
