(ns witan.send.adroddiad.vega-specs.lines
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
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
  ([xs] (five-number-summary-string {} xs))
  ([{:keys [fmt f] :or {fmt "%,.0f", f identity}} [orl irl y iru oru]]
   (apply format
          (apply format "%s (%s (%s↔%s) %s)" (repeat 5 fmt))
          (map f [y orl irl iru oru]))))

(comment
  (five-number-summary-string {:fmt "%s"} ["p05" "q1" "median" "q3" "p95"])
  ;; => "median (p05 (q1↔q3) p95)"
  (five-number-summary-string [3.0 1.0 2.0 4.0 5.0])
  ;; => "2 (3 (1↔4) 5)"
  (five-number-summary-string {:fmt "%.0f%%", :f (partial * 100)} [0.3 0.1 0.2 0.4 0.5])
  ;; => "20% (30% (10%↔40%) 50%)"

  )

(defn five-number-summary-tooltip
  [& {:keys [tooltip-field
             orl irl y iru oru]
      :or {tooltip-field :tooltip-column}
      :as cfg}]
  (fn [ds]
    (-> ds
        (tc/map-columns tooltip-field [orl irl y iru oru]
                        #(five-number-summary-string (select-keys cfg [:fmt :f]) %&)))))

(defn number-summary-tooltip
  [& {:keys [tooltip-field decimal-places]
      :or   {tooltip-field  :tooltip-column
             decimal-places 0}}]
  (fn [ds]
    (-> ds
        (tc/map-columns tooltip-field [:p05 :q1 :median :q3 :p95]
                        #(five-number-summary-string {:fmt (format "%%,.%df" decimal-places)} %&)))))

(defn pct-summary-tooltip
  [& {:keys [tooltip-field]
      :or {tooltip-field :tooltip-column}}]
  (fn [ds]
    (-> ds
        (tc/map-columns tooltip-field [:p05 :q1 :median :q3 :p95]
                        #(five-number-summary-string {:fmt "%.2f%%", :f (partial * 100)} %&)))))

(defn line-and-ribbon-and-rule-plot
  [{:keys [data
           chart-title
           x x-title x-format x-scale
           y y-title y-format y-scale y-zero
           orl irl iru oru
           tooltip-field tooltip-formatf
           group group-title
           chart-height chart-width
           colors-and-shapes]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-domain      false
           y-zero        true
           y-format      ",.0f"
           tooltip-field :tooltip-column}
    :as   cfg}]
  (let [tooltip-formatf       (or tooltip-formatf
                                  (five-number-summary-tooltip (assoc (select-keys cfg [:tooltip-field
                                                                                        :orl :irl :y :iru :oru])
                                                                      :fmt (str "%" (str/replace y-format #"%" "%%")))))
        tooltip-group-formatf (fn [g] (str g " " (->> g
                                                      (get (as-> colors-and-shapes $
                                                             (zipmap (:domain-value $) (:unicode-shape $)))))))]
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
                             :encoding {:y       {:field oru :title y-title :type "quantitative"}
                                        :y2      {:field orl}
                                        :color   {:field group :title group-title}
                                        :tooltip nil}}
                            {:mark     "errorband"
                             :encoding {:y       {:field iru :title y-title :type "quantitative"}
                                        :y2      {:field irl}
                                        :color   {:field group :title group-title}
                                        :tooltip nil}}
                            {:transform [{:filter {:param "hover" :empty false}}] :mark {:type "point" :size 200 :strokeWidth 5}}]}
                {:data     {:values (-> data
                                        tooltip-formatf
                                        (tc/order-by [x])
                                        (tc/select-columns [x group tooltip-field])
                                        (tc/map-columns group [group] tooltip-group-formatf)
                                        (tc/pivot->wider [group] [tooltip-field] {:drop-missing? false})
                                        (tc/replace-missing :all :value "")
                                        (tc/reorder-columns (cons x (:domain-value colors-and-shapes)))
                                        (tc/rows :as-maps))}
                 :mark     {:type "rule" :strokeWidth 4}
                 :encoding {:opacity {:condition {:value 0.3 :param "hover" :empty false}
                                      :value     0}
                            :tooltip (into [{:field x :type "temporal" :format x-format :title x-title}]
                                           (map (fn [g] {:field (tooltip-group-formatf g)}))
                                           (keep (into #{} (get data group)) (get colors-and-shapes :domain-value)))}
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
           y y-title y-format y-zero y-scale
           oru irl iru orl
           tooltip-field tooltip-formatf
           group group-title
           colors-and-shapes]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-format      ",.0f"
           y-zero        true
           y-scale       false
           tooltip-field :tooltip-column}
    :as   cfg}]
  (let [tooltip-formatf (or tooltip-formatf
                            (five-number-summary-tooltip (assoc (select-keys cfg [:tooltip-field
                                                                                  :orl :irl :y :iru :oru])
                                                                :fmt (str "%" (str/replace y-format #"%" "%%"))
                                                                :f   identity)))
        tooltip         [{:field group :title group-title}
                         {:field x :title x-title :type "temporal" :format x-format }
                         {:field tooltip-field :title y-title}
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
                            tooltip-formatf
                            (tc/rows :as-maps))}
     :encoding {:x {:field  x
                    :title  x-title
                    :type   "temporal"
                    :format x-format
                    :axis   {:format x-format}}
                :y {:title y-title
                    :scale {:domain y-scale
                            :zero   y-zero}}}
     :layer    [{:mark     {:type "line"
                            :size 5}
                 :encoding {:y       {:field y :type "quantitative"}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru :type "quantitative"}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field oru :type "quantitative"}
                            :y2      {:field orl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}]}))

(defn line-shape-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-zero y-scale
           oru irl iru orl
           tooltip-field tooltip-formatf
           group group-title
           colors-and-shapes]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-format      ",.0f"
           y-zero        true
           y-scale       false
           tooltip-field :tooltip-column}
    :as   cfg}]
  (let [tooltip-formatf (or tooltip-formatf
                            (five-number-summary-tooltip (assoc (select-keys cfg [:tooltip-field
                                                                                  :orl :irl :y :iru :oru])
                                                                :fmt (str "%" (str/replace y-format #"%" "%%"))
                                                                :f   identity)))
        tooltip         [{:field group :title group-title}
                         {:field x :title x-title :type "temporal" :format x-format }
                         {:field tooltip-field :title y-title}
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
                            tooltip-formatf
                            (tc/rows :as-maps))}
     :encoding {:x {:field  x
                    :title  x-title
                    :type   "temporal"
                    :format x-format
                    :axis   {:format x-format}}
                :y {:title y-title
                    :scale {:domain y-scale
                            :zero   y-zero}}}
     :layer    [{:mark     {:type  "line"
                            :size  2
                            :point {:filled      false
                                    :fill        "white"
                                    :size        50
                                    :strokewidth 0.5}}
                 :encoding {:y       {:field y :type "quantitative"}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :shape   (vs/shape-map data group colors-and-shapes)
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru :type "quantitative"}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field oru :type "quantitative"}
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
                        :scale {:domain y-scale
                                :zero   y-zero}}
                :color {:legend legend}}
     :layer    [{:mark     {:type  "line"
                            :size  2
                            :point {:filled      false
                                    :fill        "white"
                                    :size        50
                                    :strokewidth 0.5}}
                 :encoding {:y       {:field y :type "quantitative"}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (assoc (vs/color-map data group colors-and-shapes) :title group-title)
                            :shape   (vs/shape-map data group colors-and-shapes)
                            :tooltip tooltip}}]}))

(defn remove-tooltips
  "Remove tooltips from a chart:
   - Remove hover rules and associated `:transform` sub `:layer`s from a vl `chart`.
     (These are added as tooltip by `line-and-ribbon-and-rule-plot`s.)
   - Set any remaining `:tooltip` values to `nil`."
  [chart]
  (as-> chart $
    ;; Remove "hover" rule layer
    (update $ :layer (partial remove (fn [m] (and (some->> m :mark :type (= "rule"))
                                                  (some->> m :params (some #(some->> % :name (= "hover"))))))))
    ;; Remove any `:transform` layers within layers
    (update $ :layer (partial mapv (fn [m] (if (contains? m :layer)
                                             (update m :layer (partial remove #(contains? % :transform)))
                                             m))))
    ;; Disable any remaining `:tooltip`s by setting value to `nil`
    (walk/prewalk (fn [e] (if (and (map-entry? e) (= (key e) :tooltip))
                            [(key e) nil]
                            e))
                  $)))
