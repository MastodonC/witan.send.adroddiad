(ns witan.send.adroddiad.vega-specs.lines
  (:require [tablecloth.api :as tc]
            [witan.send.adroddiad.vega-specs :as vs]))

(def base-chart-spec
  {:y   :median
   :irl :q1  :iru :q3  :ir-title "50% range"
   :orl :p05 :oru :p95 :or-title "90% range"})

(defn number-summary-tooltip
  [{:keys [tooltip-field group x order-field decimal-places]
    :or {tooltip-field :tooltip-field
         x :analysis-date
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
           irl iru ir-title
           orl oru or-title
           tooltip-field tooltip-formatf
           group group-title order-field
           chart-height chart-width
           colors-and-shapes]
    :or {chart-height vs/full-height
         chart-width vs/full-width
         y-domain false
         y-zero true
         y-format ",.0f"
         tooltip-field :tooltip-column}}]
  (let [;; TODO: Find a way to get legend shape/colour into here
        tooltip-formatf (or tooltip-formatf
                            (number-summary-tooltip {:tooltip-field tooltip-field
                                                     :group group
                                                     :order-field order-field
                                                     :x x}))]
    {:data {:values (-> data
                        (tc/rows :as-maps))}
     :height chart-height
     :width chart-width
     :title {:text chart-title :fontSize 24}
     :config {:legend {:titleFontSize 20 :labelFontSize 14 :labelLimit 0}
              :axisX {:titleFontSize 16 :labelFontSize 12}
              :axisY {:titleFontSize 16 :labelFontSize 12}}
     :encoding {:x {:field x :title x-title :type "temporal"
                    :scale {:domain x-scale :x-xero false}
                    ;; :axis {:format ["%Y"] :tickCount {:interval "month" :step 12}}
                    }}
     :layer [{:encoding {:color (vs/color-map data group colors-and-shapes)
                         :shape (vs/shape-map data group colors-and-shapes)
                         :y {:field y
                             :type "quantitative"
                             :axis {:format y-format}
                             :scale {:domain y-scale :zero y-zero}}}
              :layer [{:mark {:type "line"
                              :size 5
                              :point {:filled      true #_false
                                      :size        150
                                      :strokewidth 0.5}}}
                      {:mark "errorband"
                       :encoding {:y {:field oru :title y-title :type "quantitative"}
                                  :y2 {:field orl}
                                  :color {:field group :title group-title}}}
                      {:mark "errorband"
                       :encoding {:y {:field iru :title y-title :type "quantitative"}
                                  :y2 {:field irl}
                                  :color {:field group :title group-title}}}
                      {:transform [{:filter {:param "hover" :empty false}}] :mark {:type "point" :size 200 :strokeWidth 5}}]}
             {:data {:values (tooltip-formatf data)}
              :mark {:type "rule" :strokeWidth 4}
              :encoding {:opacity {:condition {:value 0.3 :param "hover" :empty false}
                                   :value 0}
                         :tooltip (into [{:field x :type "temporal" :format x-format :title x-title}]
                                        (map (fn [g] {:field g}))
                                        (if order-field
                                          (-> data
                                              (tc/order-by [order-field])
                                              (get group))
                                          (into (sorted-set) (data group))))}
              :params [{:name "hover"
                        :select {:type "point"
                                 :size 200
                                 :fields [x]
                                 :nearest true
                                 :on "pointerover"
                                 :clear "pointerout"}}]}]}))

(defn line-and-ribbon-plot
  [{:keys [data
           chart-title chart-height chart-width
           x x-title x-format
           y y-title y-scale y-zero
           irl iru ir-title
           orl oru or-title
           group group-title
           colors-and-shapes]
    :or {chart-height vs/full-height
         chart-width  vs/full-width
         y-zero       true
         y-scale      false}}]
  {:height chart-height
   :width chart-width
   :title {:text chart-title
           :fontSize 24}
   :config {:legend {:titleFontSize 20
                     :labelFontSize 14
                     :labelLimit 0}
            :axisX {:titleFontSize 16
                    :labelFontSize 12}
            :axisY {:titleFontSize 16
                    :labelFontSize 12}}
   :data {:values (-> data
                      (tc/map-columns :ir [irl iru] (fn [lower upper]
                                                      (format "%,.1f - %,.1f" lower upper)))
                      (tc/map-columns :or [orl oru] (fn [lower upper]
                                                      (format "%,.1f - %,.1f" lower upper)))
                      (tc/rows :as-maps))}
   :encoding {:y {:scale {:domain y-scale
                          :zero   y-zero}}}
   :layer [{:mark {:type "line"
                   :size 5}
            :encoding {:y {:field y :title y-title :type "quantitative"}
                       :x {:field x :title x-title :type "temporal" :axis {:format x-format}}
                       ;; color and shape scale and range must be specified or you get extra things in the legend
                       :color (vs/color-map data group colors-and-shapes)
                       :tooltip [{:field group :title group-title}
                                 {:field x :type "temporal" :format x-format :title x-title}
                                 {:field y :title y-title}
                                 {:field :ir :title ir-title}
                                 {:field :or :title or-title}]}}
           {:mark "errorband"
            :encoding {:y {:field iru :title y-title :type "quantitative"}
                       :y2 {:field irl}
                       :x {:field x :title x-title :type "temporal" :format x-format}
                       :color {:field group :title group-title}
                       :tooltip [{:field group :title group-title}
                                 {:field x :type "temporal" :format x-format :title x-title}
                                 {:field y :title y-title}
                                 {:field :ir :title ir-title}
                                 {:field :or :title or-title}]}}
           {:mark "errorband"
            :encoding {:y {:field oru :title y-title :type "quantitative"}
                       :y2 {:field orl}
                       :x {:field x :title x-title :type "temporal" :format x-format}
                       :color {:field group :title group-title}}}]})

(defn line-shape-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-zero y-scale
           irl iru ir-title
           orl oru or-title
           group group-title
           colors-and-shapes]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           y-zero       true
           y-scale      false}}]
  {:height   chart-height
   :width    chart-width
   :title    {:text     chart-title
              :fontSize 24}
   :config   {:legend {:titleFontSize 20
                       :labelFontSize 14
                       :labelLimit 0}
              :axisX  {:titleFontSize 16
                       :labelFontSize 12}
              :axisY  {:titleFontSize 16
                       :labelFontSize 12}}
   :data     {:values (-> data
                          (tc/map-columns :ir [irl iru] (fn [lower upper]
                                                          (format "%,.1f - %,.1f" lower upper)))
                          (tc/map-columns :or [orl oru] (fn [lower upper]
                                                          (format "%,.1f - %,.1f" lower upper)))
                          (tc/rows :as-maps))}
   :encoding {:y {:scale {:domain y-scale
                          :zero   y-zero}}}
   :layer    [{:mark     {:type "line" :point {:filled      false
                                               :fill        "white"
                                               :size        50
                                               :strokewidth 0.5}}
               :encoding {:y       {:field y :title y-title :type "quantitative"}
                          :x       {:field x :title x-title :type "temporal" :axis {:format x-format}}
                          ;; color and shape scale and range must be specified or you get extra things in the legend
                          :color   (vs/color-map data group colors-and-shapes)
                          :shape   (vs/shape-map data group colors-and-shapes)
                          :tooltip [{:field group :title group-title}
                                    {:field x :type "temporal" :format x-format :title x-title}
                                    {:field y :title y-title}
                                    {:field :ir :title ir-title}
                                    {:field :or :title or-title}]}}
              {:mark     "errorband"
               :encoding {:y       {:field iru :title y-title :type "quantitative"}
                          :y2      {:field irl}
                          :x       {:field x :title x-title :type "temporal" :format x-format}
                          :color   {:field group :title group-title}
                          :tooltip [{:field group :title group-title}
                                    {:field x :type "temporal" :format x-format :title x-title}
                                    {:field y :title y-title}
                                    {:field :ir :title ir-title}
                                    {:field :or :title or-title}]}}
              {:mark     "errorband"
               :encoding {:y       {:field oru :title y-title :type "quantitative"}
                          :y2      {:field orl}
                          :x       {:field x :title x-title :type "temporal" :format x-format}
                          :color   {:field group :title group-title}
                          :tooltip [{:field group :title group-title}
                                    {:field x :type "temporal" :format x-format :title x-title}
                                    {:field y :title y-title}
                                    {:field :ir :title ir-title}
                                    {:field :or :title or-title}]}}]})

(defn line-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           legend
           x x-title x-format
           y y-title y-format
           y-zero y-scale
           group group-title
           colors-and-shapes]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           y-zero       true
           y-scale      false
           legend       true}}]
  (let [tooltip [{:field group :title group-title}
                 {:field x :type "temporal" :format x-format :title x-title}
                 {:field y :title y-title ;; :format y-format
                  }]]
    {:height   chart-height
     :width    chart-width
     :title    {:text     chart-title
                :fontSize 24}
     :config   {:legend {:titleFontSize 16
                         :labelFontSize 14
                         :labelLimit 0}
                :axisX  {:tickcount     7
                         :tickExtra     true
                         :labelalign    "center"
                         :titleFontSize 16
                         :labelFontSize 12}
                :axisY  {:titleFontSize 16
                         :labelFontSize 12}}
     :data     {:values (-> data
                            (tc/rows :as-maps))}
     :encoding {:y     {:scale {:domain y-scale
                                :zero   y-zero}}
                :color {:legend legend}}
     :layer    [{:mark     {:type "line" :point {:filled      false
                                                 :fill        "white"
                                                 :size        50
                                                 :strokewidth 0.5}}
                 :encoding {:y       {:field y :title y-title :type "quantitative"}
                            :x       {:field x :title x-title :format x-format :type "temporal"}
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
