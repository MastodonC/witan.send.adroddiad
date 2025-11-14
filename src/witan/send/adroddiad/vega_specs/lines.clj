(ns witan.send.adroddiad.vega-specs.lines
  "Specifications for Vega-Lite line plots for SEND projections."
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.vega-specs :as vs]))

(def base-chart-spec
  "Base specification mapping parameters for line and ribbon y values
   used in plot definitions to the corresponding column names
   usually used in summaries of SEND projections."
  {:y   :median
   :irl :q1  :iru :q3
   :orl :p05 :oru :p95})



;;; # tooltip helpers
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



;;; # line plots
(defn line-and-ribbon-and-rule-plot
  "Vega-Lite specs for a line, shape and ribbon plot, by `group`, with a \"hover\" vertical rule tooltip."
  [{:keys [data
           chart-title
           x x-title x-format x-scale
           y y-title y-format y-scale y-zero y-tooltip-format
           orl irl iru oru
           tooltip-field tooltip-formatf
           group group-title
           chart-height chart-width
           colors-and-shapes
           legend]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-zero        true
           y-format      ",.0f"
           tooltip-field :tooltip-column
           legend        true}
    :as   plot-spec}]
  (let [y-tooltip-format      (or y-tooltip-format y-format)
        tooltip-formatf       (or tooltip-formatf
                                  (five-number-summary-tooltip (assoc (select-keys plot-spec [:tooltip-field
                                                                                              :orl :irl :y :iru :oru])
                                                                      :fmt (str "%" (str/replace y-tooltip-format #"%" "%%")))))
        tooltip-group-formatf (fn [g] (str g " " (->> g
                                                      (get (as-> colors-and-shapes $
                                                             (zipmap (:domain-value $) (:unicode-shape $)))))))]
    {:$schema "https://vega.github.io/schema/vega-lite/v6.json"
     :title    {:text chart-title :fontSize 24}
     :data     {:values (-> data (tc/rows :as-maps))}
     :height   chart-height
     :width    chart-width
     :config   {:legend {:titleFontSize 20 :labelFontSize 14 :labelLimit 0}
                :axisX  {:titleFontSize 16 :labelFontSize 12}
                :axisY  {:titleFontSize 16 :labelFontSize 12}}
     :encoding (cond->
                {:x {:title x-title
                     :field x
                     :type  "temporal"
                     :scale {:zero false}
                     :axis  {:format    x-format
                             :tickCount {:interval "month"
                                         :step     12}}}
                 :y {:title y-title
                     :type  "quantitative"
                     :axis  {:format y-format}
                     :scale {:zero y-zero}}}
                 (boolean x-scale) (assoc-in [:x :scale :domain] x-scale)
                 (boolean y-scale) (assoc-in [:y :scale :domain] y-scale)
                 (not legend)    (assoc :color {:legend nil}))
     :layer    [{:encoding {:color (vs/color-map data group colors-and-shapes)
                            :shape (vs/shape-map data group colors-and-shapes)}
                 :layer    [{:mark     "errorband"
                             :encoding {:y     {:field oru}
                                        :y2    {:field orl}
                                        :color {:field group
                                                :title group-title}
                                        :tooltip nil}}
                            {:mark     "errorband"
                             :encoding {:y     {:field iru}
                                        :y2    {:field irl}
                                        :color {:field group
                                                :title group-title}
                                        :tooltip nil}}
                            {:mark     {:type  "line"
                                        :size  3
                                        :point {:filled      true
                                                :size        150
                                                :strokeWidth 1}}
                             :encoding {:y {:field y}}}
                            {:transform [{:filter {:param "hover"
                                                   :empty false}}]
                             :mark      {:type        "point"
                                         :size        200
                                         :strokeWidth 5}
                             :encoding  {:y {:field y}}}]}
                {:data     {:values (-> data
                                        tooltip-formatf
                                        (tc/order-by [x])
                                        (tc/select-columns [x group tooltip-field])
                                        (tc/map-columns group [group] tooltip-group-formatf)
                                        (tc/pivot->wider [group] [tooltip-field] {:drop-missing? false})
                                        (tc/replace-missing :all :value "")
                                        (tc/reorder-columns (cons x (:domain-value colors-and-shapes)))
                                        (tc/rows :as-maps))}
                 :mark     {:type "rule" :strokeWidth 10} ;; <- use wide rule while `:nearest` below isn't working
                 :encoding {:opacity {:condition {:value 0.3 :param "hover" :empty false}
                                      :value     0.01}
                            :tooltip (into [{:field x :type "temporal" :format x-format :title x-title}]
                                           (map (fn [g] {:field (tooltip-group-formatf g) :type "nominal"}))
                                           (keep (into #{} (get data group)) (get colors-and-shapes :domain-value)))}
                 :params   [{:name   "hover"
                             :select {:type    "point"
                                      :fields  [x]
                                      :nearest false #_ true ;; <- `true` resulted in `undefined` ToolTip values for groups
                                      :on      "pointerover"
                                      :clear   "pointerout"}}]}]}))

(defn line-and-ribbon-plot
  "Vega-Lite specs for a line and ribbon plot, by `group`."
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-scale y-zero y-tooltip-format
           oru irl iru orl
           tooltip-field tooltip-formatf
           group group-title
           colors-and-shapes
           legend]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-format      ",.0f"
           y-zero        true
           y-scale       false
           tooltip-field :tooltip-column
           legend        true}
    :as   plot-spec}]
  (let [y-tooltip-format (or y-tooltip-format y-format)
        tooltip-formatf  (or tooltip-formatf
                             (five-number-summary-tooltip (assoc (select-keys plot-spec [:tooltip-field
                                                                                         :orl :irl :y :iru :oru])
                                                                 :fmt (str "%" (str/replace y-tooltip-format #"%" "%%"))
                                                                 :f   identity)))
        tooltip          [{:field group :title group-title}
                          {:field x :title x-title :type "temporal" :format x-format}
                          {:field tooltip-field :title y-title}]]
    {:$schema "https://vega.github.io/schema/vega-lite/v6.json"
     :height   chart-height
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
     :encoding {:x     {:field  x
                        :title  x-title
                        :type   "temporal"
                        :format x-format
                        :axis   {:format x-format}}
                :y     {:title y-title
                        :scale {:domain y-scale
                                :zero   y-zero}
                        :axis   {:format y-format}}
                :color {:legend legend}}
     :layer    [{:mark     "errorband"
                 :encoding {:y       {:field oru :type "quantitative"}
                            :y2      {:field orl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru :type "quantitative"}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     {:type "line"
                            :size 5}
                 :encoding {:y       {:field y :type "quantitative"}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :tooltip tooltip}}]}))

(defn line-shape-and-ribbon-plot
  "Vega-Lite specs for a line, shape and ribbon plot, by `group`."
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-scale y-zero y-tooltip-format
           oru irl iru orl
           tooltip-field tooltip-formatf
           group group-title
           colors-and-shapes
           legend]
    :or   {chart-height  vs/full-height
           chart-width   vs/full-width
           y-format      ",.0f"
           y-zero        true
           y-scale       false
           tooltip-field :tooltip-column
           legend        true}
    :as   plot-spec}]
  (let [y-tooltip-format (or y-tooltip-format y-format)
        tooltip-formatf  (or tooltip-formatf
                             (five-number-summary-tooltip (assoc (select-keys plot-spec [:tooltip-field
                                                                                         :orl :irl :y :iru :oru])
                                                                 :fmt (str "%" (str/replace y-tooltip-format #"%" "%%"))
                                                                 :f   identity)))
        tooltip         [{:field group :title group-title}
                         {:field x :title x-title :type "temporal" :format x-format}
                         {:field tooltip-field :title y-title}]]
    {:$schema "https://vega.github.io/schema/vega-lite/v6.json"
     :height   chart-height
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
     :encoding {:x     {:field  x
                        :title  x-title
                        :type   "temporal"
                        :format x-format
                        :axis   {:format x-format}}
                :y     {:title y-title
                        :scale {:domain y-scale
                                :zero   y-zero}
                        :axis   {:format y-format}}
                :color {:legend legend}}
     :layer    [{:mark     "errorband"
                 :encoding {:y       {:field oru :type "quantitative"}
                            :y2      {:field orl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     "errorband"
                 :encoding {:y       {:field iru :type "quantitative"}
                            :y2      {:field irl}
                            :color   {:field group :title group-title}
                            :tooltip tooltip}}
                {:mark     {:type  "line"
                            :size  2
                            :point {:filled      false
                                    :fill        "white"
                                    :size        50
                                    :strokewidth 0.5}}
                 :encoding {:y       {:field y :type "quantitative"}
                            ;; color and shape scale and range must be specified or you get extra things in the legend
                            :color   (vs/color-map data group colors-and-shapes)
                            :shape   (vs/shape-map data group colors-and-shapes)
                            :tooltip tooltip}}]}))

(defn line-plot
  "Vega-Lite specs for a shape and line plot, by `group`."
  [{:keys [data
           chart-title
           chart-height chart-width
           x x-title x-format
           y y-title y-format y-scale y-zero y-tooltip-format
           group group-title
           colors-and-shapes
           legend]
    :or   {chart-height vs/full-height
           chart-width  vs/full-width
           y-format     ",.0f"
           y-zero       true
           y-scale      false
           legend       true}}]
  (let [y-tooltip-format (or y-tooltip-format y-format)
        tooltip          [{:field group :title group-title}
                          {:field x :title x-title :type "temporal"     :format x-format}
                          {:field y :title y-title :type "quantitative" :format y-tooltip-format}]]
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
                                :zero   y-zero}
                        :axis   {:format y-format}}
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



;;; # Helper functions
(defn plot-spec-by-group->plot-spec-by-group-label
  "Converts a `plot-spec` with `group`s identified by abbreviations to use labels:
   Uses `:label` column from `colors-and-shapes` (if present) to update `data` 
   `group` `colors-and-shapes` in a `plot-spec` to group by the labelled values
   as follows:
   - The (domain) values in the `group` column of the `data` are mapped to the 
     corresponding labels using the `colors-and-shapes` `:domain-value` -> `:label` 
     mapping.
   - These group labels are added to the `data` in column `group-label`.
     - Any pre-existing column of this name will be overwritten.
     - Specify as the `group` column to have the `group` column abbreviations
       replaced with labels.
     - Defaults to name of `group` column with `-label` appended (if keyword).
   - The `colors-and-shapes` dataset is updated with the `:label`s as new `:domain-values`.
   - The `group` key is updated to refer to the new `group-label` column.
   - Any `group-label` key provided is removed from the `plot-spec`."
  [{:keys [data group colors-and-shapes group-label]
    :or   {group-label nil}
    :as   plot-spec}]
  (let [group-label (or group-label
                        (cond (keyword? group) (-> group name (str "-label") keyword)
                                (string?  group) (-> group (str " Label"))))]
    (-> plot-spec
        (dissoc :group-label)
        (merge (when (some #{:label} (tc/column-names colors-and-shapes))
                 (let [domain-value->label (as-> colors-and-shapes $ (zipmap ($ :domain-value) ($ :label)))]
                   {:data              (as-> data $
                                         (tc/map-columns $ group-label [group] #(get domain-value->label % %))
                                         (tc/reorder-columns $ (concat (take-while (complement #{group}) (tc/column-names $))
                                                                       [group group-label])))
                    :group             group-label
                    :colors-and-shapes (-> colors-and-shapes
                                           (tc/drop-columns [:domain-value])
                                           (tc/rename-columns {:label :domain-value}))}))))))

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

(defn process-colors-and-shapes
  "Processes a `plot-spec` to:
   - provide a default `colors-and-shapes` (for the `group` values in the `data`) if none specified,
   - throw an exception if the supplied `:colors-and-shapes` doesn't contain all `group`s in the `data`, and
   - call `plot-spec-by-group->plot-spec-by-group-label` to apply any labels in the `colors-and-shapes` to the `group` values."
  [{:keys [data group colors-and-shapes] :as plot-spec}]
  (let [data-groups                       (-> data (get group) set)
        domain-values                     (-> colors-and-shapes :domain-value set)
        data-groups-without-domain-values (set/difference data-groups domain-values)]
    (cond
      ;; make `:colors-and-shapes` if none specified:
      (nil? colors-and-shapes)
      (assoc plot-spec :colors-and-shapes (vs/color-and-shape-lookup data-groups))
      ;; if supplied `:colors-and-shapes` doesn't contain all `group`s in the `data` then error:
      (seq data-groups-without-domain-values)
      (throw (ex-info (str "`colors-and-shapes` doesn't contain all `group`s in the `data`.")
                      {:group                             group
                       :colors-and-shapes                 colors-and-shapes
                       :data-groups-without-domain-values data-groups-without-domain-values}))
      ;; otherwise process the plot-spec to apply any labels specified:
      :else
      (plot-spec-by-group->plot-spec-by-group-label plot-spec))))



;;; # Wrappers
(defn plot-ehcps-against-year-by-group
  "Wrapper for `line*-plot`s of `data` by `group` supplying defaults for plotting an EHCP projection summary.

   Augments supplied parameter map with defaults suitable to plot `data`
   7-number summary of #EHCP projections (y-axis) against year (x-axis) grouped
   by `group` (i.e. with a separate line for each value of `group`), then:
   - calls `process-colors-and-shapes` to:
     - provide a default `colors-and-shapes` if none specified, and
     - apply any labels;
   - ensures the year `x` variable is a string (for vega-lite), and
   - passes through to a specific `plotf` to return a Vega-Lite spec.

   Note:
   - Only requires: `data` & `group`.
   - Other parameters (except `plotf`) are passed through to `plotf`
     (overriding any defaults specified here)."
  [& {:keys [data group group-title plotf]
      :or   {group-title nil
             plotf       line-and-ribbon-and-rule-plot}
      :as   plot-spec}]
  (-> base-chart-spec
      ;; defaults
      (merge {:chart-title  (str "#EHCPs by " (or group-title (name group)))
              :chart-height vs/full-height
              :chart-width  vs/two-thirds-width
              :x            :calendar-year
              :x-format     "%Y"
              :x-title      "Census Year"
              :y-title      "# EHCPs"
              :y-zero       true
              :y-scale      false
              :group-title  (or group-title (name group))})
      ;; over-ride with supplied `plot-spec`
      (merge plot-spec)
      ;; process `colors-and-shapes`
      process-colors-and-shapes
      ;; convert the `x` column of the `data` to `str`
      ((fn [m] (update m :data
                       (fn [ds] (tc/update-columns ds [(:x m)] (partial map str))))))
      ;; call the specified `plotf`
      (dissoc :plotf)
      plotf))
