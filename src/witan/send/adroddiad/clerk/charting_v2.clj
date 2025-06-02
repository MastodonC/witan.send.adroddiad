(ns witan.send.adroddiad.clerk.charting-v2
  (:require
   [clojure2d.color :as color]
   [nextjournal.clerk :as clerk]
   [clojure.string :as string]
   [tablecloth.api :as tc]
   [tech.v3.datatype.functional :as dfn]
   [witan.send.adroddiad.dataset :as ds]
   [witan.send.adroddiad.transitions :as tr]))

(def full-height 600)
(def two-rows 200)
(def half-width 600)
(def full-width 1420)

(defn color-and-shape-lookup [domain]
  ;; Using Tableau 20 palette, excluding the red, ordered with
  ;; Tableau 10 shades first, then the lighter shades in the same order.
  ;; With 19 colours and 8 shapes, this gives 152 distinct combinations.
  (tc/dataset
   {:domain-value domain
    ;; I’d like to eventually do something based on these colours in v3
    ;; ["#fa814c" "#256cc6" "#fbe44c" "#50b938" "#59c4b8" "#29733c"]
    :color (cycle (map color/format-hex
                       [;; tableau 10
                        [ 31 119 180 255]
                        [255 127  14 255]
                        [ 44 160  44 255]
                        #_[214  39  40 255]
                        [148 103 189 255]
                        [140  86  75 255]
                        [227 119 194 255]
                        [127 127 127 255]
                        [188 189  34 255]
                        [ 23 190 207 255]
                        ;; tableau 20 lighter shades
                        [174 199 232 255]
                        [255 187 120 255]
                        [152 223 138 255]
                        [255 152 150 255]
                        [197 176 213 255]
                        [196 156 148 255]
                        [247 182 210 255]
                        [199 199 199 255]
                        [219 219 141 255]
                        [158 218 229 255]]))
    :shape (cycle ["circle"
                   "square"
                   "triangle-up"
                   "triangle-down"
                   "triangle-right"
                   "triangle-left"
                   "cross"
                   "diamond"])}))

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
           clerk-width legend
           x x-title x-format
           y y-title y-format
           y-zero y-scale
           group group-title
           colors-and-shapes
           labelLimit]
    :or {chart-height full-height
         chart-width full-width
         clerk-width :full
         y-zero true
         y-scale false
         legend {:encode {:labels {:update {:text {:signal "[datum.value]"}}}}}
         labelLimit 0}}]
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
                        :labelFontSize 14
                        :labelLimit labelLimit}
               :axisX {:tickcount 7
                       :tickExtra true
                       :labelalign "center"
                       :titleFontSize 16
                       :labelFontSize 12}
               :axisY {:titleFontSize 16
                       :labelFontSize 12}}
      :data {:values (-> data
                         (tc/rows :as-maps))}
      :encoding {:y {:scale {:domain y-scale
                             :zero y-zero}}
                 :color {:legend (cond
                                   (false? legend)
                                   false
                                   :else
                                   legend)}}
      :layer [{:mark {:type "line", :point {:filled false,
                                            :fill "white",
                                            :size 50
                                            :strokewidth 0.5}},
               :encoding {:y {:field y, :title y-title :type "quantitative"},
                          :x {:field x, :title x-title :format x-format :type "temporal"},
                          ;; color and shape scale and range must be specified or you get extra things in the legend
                          :color (assoc (color-map data group colors-and-shapes) :title group-title)
                          :shape (shape-map data group colors-and-shapes)
                          :tooltip tooltip}}]})))

(defn line-and-ribbon-plot
  [{:keys [data
           chart-title
           chart-height chart-width
           clerk-width legend
           x x-title x-format
           y y-title y-format
           y-zero y-scale
           irl iru ir-title
           orl oru or-title
           range-format-f
           group group-title
           colors-and-shapes
           labelLimit]
    :or {chart-height full-height
         chart-width full-width
         clerk-width :full
         range-format-f (fn [lower upper]
                          (format "%,f - %,1f" lower upper))
         y-zero true
         y-scale false
         legend {:encode {:labels {:update {:text {:signal "[datum.value]"}}}}}
         labelLimit 0}}]
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
                        :labelFontSize 14
                        :labelLimit labelLimit}
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
      :encoding {:y {:scale {:domain y-scale
                             :zero y-zero}}
                 :color {:legend (cond
                                   (false? legend)
                                   false
                                   :else
                                   legend)}}
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
               :encoding {:y {:field y, :title y-title :type "quantitative"},
                          :x {:field x, :title x-title :format x-format :type "temporal"},
                          ;; color and shape scale and range must be specified or you get extra things in the legend
                          :color (color-map data group colors-and-shapes)
                          :shape (shape-map data group colors-and-shapes)
                          :tooltip tooltip}}]})))

(def field-descriptions
  {:calendar-year   :calendar-year-label
   :setting         :setting-label
   :setting-1       :setting-1-label
   :setting-2       :setting-2-label
   :designation     :designation-label
   :need            :need-label
   :need-1          :need-1-label
   :need-2          :need-2-label
   :academic-year   :academic-year-label
   :academic-year-1 :academic-year-1-label
   :academic-year-2 :academic-year-2-label
   :key-stage       :key-stage-label
   :key-stage-1     :key-stage-1-label
   :key-stage-2     :key-stage-2-label
   :school-phase    :school-phase-label
   :school-phase-1  :school-phase-1-label
   :school-phase-2  :school-phase-2-label
   :scenario        :scenario-label})

(def sweet-column-names
  {:calendar-year         "Calendar Year"
   :diff                  "Count"
   :row-count             "Row Count"
   :pct-change            "% Change"
   :setting-label         "Setting"
   :setting               "Setting"
   :need-label            "Need"
   :need                  "Need"
   :academic-year         "NCY"
   :academic-year-label   "NCY"
   :setting-label-1       "Setting 1"
   :need-label-1          "Need 1"
   :academic-year-1       "NCY 1"
   :academic-year-1-label "NCY 1"
   :setting-label-2       "Setting 2"
   :need-label-2          "Need 2"
   :academic-year-2-label "NCY 2"
   :setting-1             "Setting 1"
   :setting-2             "Setting 2"
   :key-stage             "Key Stage"
   :key-stage-1           "Key Stage (from)"
   :key-stage-2           "Key Stage (to)"
   :school-phase          "School Phase"
   :school-phase-1        "School Phase 1"
   :school-phase-2        "School Phase 2"
   :scenario-label        "Scenario"
   :scenario              "EHCP Count"})

(def axis-labels
  {:setting         "Setting"
   :designation     "Designation"
   :need            "Need"
   :calendar-year   "Calendar Year"
   :setting-1       "Setting 1"
   :setting-2       "Setting 2"
   :academic-year-1 "NCY 1"
   :academic-year-2 "NCY 2"
   :academic-year   "NCY"
   :key-stage       "Key Stage"
   :key-stage-1     "Key Stage (from)"
   :key-stage-2     "Key Stage (to)"
   :school-phase    "School Phase"
   :school-phase-1  "School Phase 1"
   :school-phase-2  "School Phase 2"
   :scenario        "EHCP Count"})

(def sort-field
  {:setting         :setting-order
   :setting-1       :setting-1-order
   :setting-2       :setting-2-order
   :designation     :designation-order
   :need            :need-order
   :need-1          :need-1-order
   :need-2          :need-2-order
   :academic-year   :academic-year-order
   :academic-year-1 :academic-year-1-order
   :academic-year-2 :academic-year-2-order
   :key-stage       :key-stage-order
   :key-stage-1     :key-stage-1-order
   :key-stage-2     :key-stage-2-order
   :school-phase    :school-phase-order
   :school-phase-1  :school-phase-1-order
   :school-phase-2  :school-phase-2-order})

(def transitions-labels->census-labels
  {:setting-1       :setting
   :setting-2       :setting
   :need-1          :need
   :need-2          :need
   :academic-year-1 :academic-year
   :academic-year-2 :academic-year
   :key-stage-1     :key-stage
   :key-stage-2     :key-stage
   :school-phase-1  :school-phase
   :school-phase-2  :school-phase})

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
  (let [tooltip [{:field (or y-field-desc y-field) :type "ordinal" :title y-field-label}
                 {:field (or x-field-desc x-field) :type "ordinal" :title x-field-label}
                 {:field color-field :type "quantitative"}]]
    {:data {:values data}
     :encoding {:x {:field (or x-field-desc x-field) :type "nominal" :sort {:field (or x-sort-field x-field)} :title (or x-field-label x-field)}
                :y {:field (or y-field-desc y-field) :type "ordinal" :sort {:field (or y-sort-field y-field)} :title (or y-field-label y-field)}}
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

(defn ehcp-heatmap-per-year
  [census x-field]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field     "Row Count"
         x-field         x-field
         x-order-field   (sort-field x-field)
         x-value->order  (zipmap (census x-field) (census x-order-field))
         x-desc-field    (field-descriptions x-field)
         x-value->desc   (zipmap (census x-field) (census x-desc-field))
         y-field         :calendar-year
         data            (-> census
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0)
                             (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                             (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                             (tc/order-by [x-order-field :calendar-year]))
         white-text-test (format "datum['%s'] > %d"
                                 (name color-field)
                                 (int (+ (reduce dfn/min (data color-field))
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          150
       :width           (cond
                          (= x-field :academic-year)
                          full-width
                          (= x-field :scenario)
                          two-rows
                          :else
                          half-width)
       :y-field         y-field
       :y-field-label   (axis-labels y-field)
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   (axis-labels x-field)
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           (str "# EHCPs for " (sweet-column-names x-field x-field) " by Year")
       :white-text-test white-text-test}))))

(defn ehcps-total-by-year
  ([census {:keys [scenario] :as opts :or {scenario "Baseline"}}]
   (-> census
       (tc/add-column :scenario scenario)
       (ehcp-heatmap-per-year :scenario)))
  ([census]
   (ehcps-total-by-year census {})))

(defn ehcps-by-setting-per-year
  [census]
  (ehcp-heatmap-per-year census :setting))

(defn ehcp-by-need-by-year
  [census]
  (ehcp-heatmap-per-year census :need))

(defn ehcps-by-school-phase-per-year
  [census]
  (ehcp-heatmap-per-year census :school-phase))

(defn ehcps-by-key-stage-per-year
  [census]
  (ehcp-heatmap-per-year census :key-stage))

(defn ehcps-by-ncy-per-year
  [census]
  (ehcp-heatmap-per-year census :academic-year))

(defn ehcps-yoy-change
  [census domain]
  (let [most-recent-year (reduce dfn/max (:calendar-year census))
        domain-order-field (sort-field domain)
        domain-value->order (zipmap (census domain) (census domain-order-field))
        domain-desc-field (field-descriptions domain)
        domain-value->desc (zipmap (census domain) (census domain-desc-field))]
    (clerk/vl
     {:data {:values (as-> census $
                       (tc/group-by $ [domain :calendar-year])
                       (tc/aggregate $ {:count tc/row-count})
                       (tc/order-by $ [domain :calendar-year])
                       (tc/group-by $ [domain] {:result-type :as-map})
                       (map #(ds/add-diff-and-pct-diff (val %) :count :calendar-year) $)
                       (apply tc/concat $)
                       (tc/replace-missing $ :diff :value 0)
                       (tc/select-rows $ #(= most-recent-year (:calendar-year %)))
                       (tc/map-columns $ domain-order-field [domain] #(get domain-value->order % %))
                       (tc/map-columns $ domain-desc-field [domain] #(get domain-value->desc % %))
                       (tc/order-by $ [domain-order-field :calendar-year])
                       (tc/rename-columns $ {:diff "Count"})
                       (tc/rows $ :as-maps))}
      :title {:text  (format "%d→%d YoY EHCP Count Change by %s"
                             (- most-recent-year 1) most-recent-year
                             (axis-labels domain))
              :fontsize 24}
      :height full-height
      :width half-width
      :encoding {:x {:field "Count" :type "quantitative"}
                 :y {:field (or domain-desc-field domain), :type "ordinal", :sort {:field (or domain-order-field domain)}, :title (axis-labels domain)}
                 :tooltip [{:field domain-desc-field, :type "nominal", :title (sweet-column-names domain)},
                           {:field "Count", :title "Count"}]}
      :mark "bar"})))

(defn echps-total-yoy-change
  [census]
  (let [min-year (reduce dfn/min (:calendar-year census))]
    (clerk/vl
     {:data {:values (as-> census $
                       (tc/group-by $ [:calendar-year])
                       (tc/aggregate $ {:count tc/row-count})
                       (tc/order-by $ [:calendar-year])
                       (ds/add-diff-and-pct-diff $ :count :calendar-year)
                       (tc/replace-missing $ :diff :value 0)
                       (tc/order-by $ [:calendar-year])
                       (tc/rename-columns $ {:diff "Count"})
                       (tc/drop-rows $ #(= min-year (:calendar-year %)))
                       (tc/rows $ :as-maps))}
      :title {:text  "YoY EHCP Count Change"
              :fontsize 24}
      :height full-height
      :width 400
      :encoding {:x {:field "Count" :type "quantitative"}
                 :y {:field :calendar-year, :type "ordinal", :sort "ascending", :title (axis-labels :calendar-year)}
                 :tooltip [{:field :calendar-year, :type "nominal", :title (sweet-column-names :calendar-year)},
                           {:field "Count", :title "Count"}]}
      :mark "bar"})))

(defn ehcps-by-setting-yoy-change
  [census]
  (ehcps-yoy-change census :setting))

(defn ehcps-by-need-yoy-change
  [census]
  (ehcps-yoy-change census :need))

(defn ehcps-by-school-phase-yoy-change
  [census]
  (ehcps-yoy-change census :school-phase))

(defn ehcps-by-key-stage-yoy-change
  [census]
  (ehcps-yoy-change census :key-stage))

(defn ehcps-by-ncy-yoy-change
  [census]
  (ehcps-yoy-change census :academic-year))

(defn ehcp-yoy-pct-change
  [census domain]
  (let [most-recent-year (reduce dfn/max (:calendar-year census))
        domain-order-field (sort-field domain)
        domain-value->order (zipmap (census domain) (census domain-order-field))
        domain-desc-field (field-descriptions domain)
        domain-value->desc (zipmap (census domain) (census domain-desc-field))]
    (clerk/vl
     {:data {:values (as-> census $
                       (tc/group-by $ [domain :calendar-year])
                       (tc/aggregate $ {:count tc/row-count})
                       (tc/order-by $ [domain :calendar-year])
                       (tc/group-by $ [domain] {:result-type :as-map})
                       (map #(ds/add-diff-and-pct-diff (val %) :count :calendar-year) $)
                       (apply tc/concat $)
                       (tc/replace-missing $ :pct-change)
                       (tc/select-rows $ #(= most-recent-year (:calendar-year %)))
                       (tc/map-columns $ domain-order-field [domain] #(get domain-value->order % %))
                       (tc/map-columns $ domain-desc-field [domain] #(get domain-value->desc % %))
                       (tc/order-by $ [domain-order-field :calendar-year])
                       (tc/rename-columns $ {:pct-change "% Change"})
                       (tc/rows $ :as-maps))}
      :title {:text  (format "%d→%d YoY EHCP Percentage Change by %s"
                             (- most-recent-year 1) most-recent-year
                             (axis-labels domain))
              :fontsize 24}
      :height full-height
      :width half-width
      :encoding {:x {:field "% Change" :type "quantitative"}
                 :y {:field domain-desc-field, :type "ordinal", :sort {:field (or domain-order-field domain)}, :title (axis-labels domain)}
                 :tooltip [{:field domain-desc-field, :type "nominal", :title (sweet-column-names domain)},
                           {:field "% Change", :title "% Change"}]}
      :mark "bar"})))

(defn echps-total-yoy-pct-change
  [census]
  (let [min-year (reduce dfn/min (:calendar-year census))]
    (clerk/vl
     {:data {:values (as-> census $
                       (tc/group-by $ [:calendar-year])
                       (tc/aggregate $ {:count tc/row-count})
                       (tc/order-by $ [:calendar-year])
                       (ds/add-diff-and-pct-diff $ :count :calendar-year)
                       (tc/replace-missing $ :pct-change :value 0)
                       (tc/order-by $ [:calendar-year])
                       (tc/rename-columns $ {:pct-change "% Change"})
                       (tc/drop-rows $ #(= min-year (:calendar-year %)))
                       (tc/rows $ :as-maps))}
      :title {:text  "YoY EHCP Percentage Change"
              :fontsize 24}
      :height full-height
      :width 400
      :encoding {:x {:field "% Change" :type "quantitative"}
                 :y {:field :calendar-year, :type "ordinal", :sort "ascending", :title (axis-labels :calendar-year)}
                 :tooltip [{:field :calendar-year, :type "nominal", :title (sweet-column-names :calendar-year)},
                           {:field "% Change", :title "% Change"}]}
      :mark "bar"})))

(defn ehcps-by-setting-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :setting))

(defn ehcps-by-need-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :need))

(defn ehcps-by-school-phase-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :school-phase))

(defn ehcps-by-key-stage-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :key-stage))

(defn ehcps-by-key-stage-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :key-stage))

(defn ehcps-by-ncy-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :academic-year))

(defn transitions-heatmap-per-year
  [transitions x-field predicate]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field     "Row Count"
         y-field         :calendar-year
         x-order-field   (sort-field x-field)
         x-value->order  (zipmap (transitions x-field) (transitions x-order-field))
         y-desc-field     (field-descriptions y-field)
         x-desc-field    (field-descriptions x-field)
         x-value->desc   (zipmap (transitions x-field) (transitions x-desc-field))
         x-field-label   (as-> x-field $ (transitions-labels->census-labels $ $) (axis-labels $ $))
         title           (str "# "
                              (cond
                                (= predicate tr/joiner?) "Joiners"
                                (= predicate tr/leaver?) "Leavers")
                              " by " x-field-label " by Year "
                              (cond
                                (= predicate tr/joiner?) "Joined"
                                (= predicate tr/leaver?) "Left"))
         data            (-> transitions
                             (tc/select-rows predicate)
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0)
                             (tc/map-columns :calendar-year [:calendar-year]
                                             (fn [cy]
                                               (cond
                                                 (= predicate tr/joiner?) (inc cy)
                                                 (= predicate tr/leaver?) cy)))
                             (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                             (tc/map-columns y-desc-field [y-field]
                                             #(cond
                                                (= predicate tr/joiner?) (format "→%d" %)
                                                (= predicate tr/leaver?) (format "%d→" %)))
                             (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                             (tc/order-by [x-order-field :calendar-year]))
         white-text-test (format "datum['%s'] > %d"
                                 (name color-field)
                                 (int (+ (reduce dfn/min (data color-field))
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          two-rows
       :width           (cond
                          (or (= x-field :academic-year-1)
                              (= x-field :academic-year-2))
                          full-width
                          (= x-field :scenario)
                          two-rows
                          :else
                          half-width)
       :y-field         y-field
       :y-field-desc    y-desc-field
       :y-field-label   (axis-labels y-field)
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   x-field-label
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           title
       :white-text-test white-text-test}))))

(defn joiners-by-ehcp-per-year
  ([transitions {:keys [scenario] :as opts :or {scenario "Baseline"}}]
   (-> transitions
       (tc/add-column :scenario scenario)
       (transitions-heatmap-per-year :scenario tr/joiner?)))
  ([transitions]
   (joiners-by-ehcp-per-year transitions {})))

(defn joiners-by-school-phase-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :school-phase-2 tr/joiner?))

(defn joiners-by-key-stage-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :key-stage-2 tr/joiner?))

(defn joiners-by-ncy-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :academic-year-2 tr/joiner?))

(defn joiners-by-need-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :need-2 tr/joiner?))

(defn joiners-by-setting-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :setting-2 tr/joiner?))

(defn leavers-by-ehcp-per-year
  ([transitions {:keys [scenario] :as opts :or {scenario "Baseline"}}]
   (-> transitions
       (tc/add-column :scenario scenario)
       (transitions-heatmap-per-year :scenario witan.send.adroddiad.transitions/leaver?)))
  ([transitions]
   (leavers-by-ehcp-per-year transitions {})))

(defn leavers-by-school-phase-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :school-phase-1 tr/leaver?))

(defn leavers-by-key-stage-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :key-stage-1 tr/leaver?))

(defn leavers-by-ncy-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :academic-year-1 tr/leaver?))

(defn leavers-by-need-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :need-1 tr/leaver?))

(defn leavers-by-setting-per-year
  [transitions]
  (transitions-heatmap-per-year transitions :setting-1 tr/leaver?))

(defn setting-to-setting-heatmap
  [transitions]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field      "Row Count"
         most-recent-year (reduce dfn/max (:calendar-year transitions))
         y-field          :setting-1
         x-field          :setting-2
         y-order-field    :setting-1-order
         x-order-field    :setting-2-order
         y-value->order   (zipmap (transitions y-field) (transitions y-order-field))
         x-value->order   (zipmap (transitions x-field) (transitions x-order-field))
         y-desc-field     (field-descriptions y-field)
         x-desc-field     (field-descriptions x-field)
         y-value->desc    (zipmap (transitions y-field) (transitions y-desc-field))
         x-value->desc    (zipmap (transitions x-field) (transitions x-desc-field))
         data             (-> transitions
                              (tc/select-rows #(= most-recent-year (% :calendar-year)))
                              (tc/group-by [x-field y-field])
                              (tc/aggregate {color-field tc/row-count})
                              (tc/complete x-field y-field)
                              (tc/replace-missing color-field :value 0)
                              (tc/map-columns y-order-field [y-field] #(get y-value->order % %))
                              (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                              (tc/map-columns y-desc-field [y-field] #(get y-value->desc % %))
                              (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                              (tc/order-by [x-order-field y-order-field]))
         white-text-test  (format "datum['%s'] > %d"
                                  (name color-field)
                                  (int (+ (reduce dfn/min (data color-field))
                                          (* 0.450 (- (reduce dfn/max (data color-field))
                                                      (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          full-height
       :width           full-width
       :y-field         y-field
       :y-field-desc    y-desc-field
       :y-field-label   (str most-recent-year " " (-> y-field transitions-labels->census-labels axis-labels))
       :y-sort-field    y-order-field
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   (str (inc most-recent-year) " " (-> x-field transitions-labels->census-labels axis-labels))
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           (str most-recent-year "→" (+ most-recent-year 1) " Setting to Setting Transitions")
       :white-text-test white-text-test}))))

(defn setting-mover-heatmap
  [transitions]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field      "Row Count"
         most-recent-year (reduce dfn/max (:calendar-year transitions))
         y-field          :setting-1
         x-field          :setting-2
         y-order-field    :setting-1-order
         x-order-field    :setting-2-order
         y-value->order   (zipmap (transitions y-field) (transitions y-order-field))
         x-value->order   (zipmap (transitions x-field) (transitions x-order-field))
         y-desc-field     (field-descriptions y-field)
         x-desc-field     (field-descriptions x-field)
         y-value->desc    (zipmap (transitions y-field) (transitions y-desc-field))
         x-value->desc    (zipmap (transitions x-field) (transitions x-desc-field))
         data             (-> transitions
                              (tc/select-rows #(= most-recent-year (% :calendar-year)))
                              (tc/select-rows tr/mover?)
                              (tc/group-by [x-field y-field])
                              (tc/aggregate {color-field tc/row-count})
                              (tc/complete x-field y-field)
                              (tc/replace-missing color-field :value 0)
                              (tc/map-columns y-order-field [y-field] #(get y-value->order % %))
                              (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                              (tc/map-columns y-desc-field [y-field] #(get y-value->desc % %))
                              (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                              (tc/order-by [x-order-field y-order-field]))
         white-text-test  (format "datum['%s'] > %d"
                                  (name color-field)
                                  (int (+ (reduce dfn/min (data color-field))
                                          (* 0.450 (- (reduce dfn/max (data color-field))
                                                      (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          full-height
       :width           full-width
       :y-field         y-field
       :y-field-desc    y-desc-field
       :y-field-label   (str most-recent-year " " (-> y-field transitions-labels->census-labels axis-labels))
       :y-sort-field    y-order-field
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   (str (inc most-recent-year) " " (-> x-field transitions-labels->census-labels axis-labels))
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           (str most-recent-year "→" (+ most-recent-year 1) " Setting to Setting Movers")
       :white-text-test white-text-test}))))

(defn joiners-by-two-domains
  [transitions x-field y-field]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field      "Row Count"
         most-recent-year (reduce dfn/max (:calendar-year transitions))
         y-order-field   (sort-field y-field)
         x-order-field   (sort-field x-field)
         y-value->order   (zipmap (transitions y-field) (transitions y-order-field))
         x-value->order   (zipmap (transitions x-field) (transitions x-order-field))
         y-desc-field     (field-descriptions y-field)
         x-desc-field     (field-descriptions x-field)
         y-value->desc    (zipmap (transitions y-field) (transitions y-desc-field))
         x-value->desc    (zipmap (transitions x-field) (transitions x-desc-field))
         y-field-label   (as-> y-field $ (transitions-labels->census-labels $ $) (axis-labels $ $))
         x-field-label   (as-> x-field $ (transitions-labels->census-labels $ $) (axis-labels $ $))
         data            (-> transitions
                             (tc/select-rows #(and (= "NONSEND" (:setting-1 %))
                                                   (= most-recent-year (% :calendar-year))))
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0)
                             (tc/map-columns y-order-field [y-field] #(get y-value->order % %))
                             (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                             (tc/map-columns y-desc-field [y-field] #(get y-value->desc % %))
                             (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                             (tc/order-by [x-order-field y-order-field]))
         white-text-test (format "datum['%s'] > %d"
                                 (name color-field)
                                 (int (+ (reduce dfn/min (data color-field))
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          full-height
       :width           full-width
       :y-field         y-field
       :y-field-desc    y-desc-field
       :y-field-label   y-field-label
       :y-sort-field    y-order-field
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   x-field-label
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           (format "New EHCPs %d→%d by %s and %s"
                                most-recent-year (inc most-recent-year)
                                y-field-label x-field-label)
       :white-text-test white-text-test}))))

(defn joiners-by-setting-and-school-phase
  [transitions]
  (joiners-by-two-domains transitions :school-phase-2 :setting-2))

(defn joiners-by-setting-and-key-stage
  [transitions]
  (joiners-by-two-domains transitions :key-stage-2 :setting-2))

(defn joiners-by-setting-and-ncy
  [transitions]
  (joiners-by-two-domains transitions :academic-year-2 :setting-2))

(defn needs-by-designation
  [census]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field      "Row Count"
         most-recent-year (reduce dfn/max (:calendar-year census))
         y-field          :designation
         x-field          :need
         y-order-field    (sort-field y-field)
         x-order-field    (sort-field x-field)
         y-value->order   (zipmap (census y-field) (census y-order-field))
         x-value->order   (zipmap (census x-field) (census x-order-field))
         y-desc-field     (field-descriptions y-field)
         x-desc-field     (field-descriptions x-field)
         y-value->desc    (zipmap (census y-field) (census y-desc-field))
         x-value->desc    (zipmap (census x-field) (census x-desc-field))
         data             (-> census
                              (tc/map-columns :designation [:setting]
                                              (fn [s] (cond
                                                        (clojure.string/includes? s "_")
                                                        (-> s
                                                            (clojure.string/split #"_")
                                                            second)
                                                        :else nil)))
                              (tc/drop-missing [:designation])
                              (tc/drop-rows (comp #{"InA" "OoA"} :designation))
                              (tc/select-rows #(= most-recent-year (:calendar-year %)))
                              (tc/group-by [x-field y-field])
                              (tc/aggregate {"Row Count" tc/row-count})
                              (tc/complete x-field y-field)
                              (tc/replace-missing "Row Count" :value 0)
                              (tc/map-columns y-order-field [y-field] #(get y-value->order % %))
                              (tc/map-columns x-order-field [x-field] #(get x-value->order % %))
                              (tc/map-columns y-desc-field [y-field] #(get y-value->desc % %))
                              (tc/map-columns x-desc-field [x-field] #(get x-value->desc % %))
                              (tc/order-by [x-order-field y-order-field]))
         white-text-test  (format "datum['%s'] > %d"
                                  (name color-field)
                                  (int (+ (reduce dfn/min (data color-field))
                                          (* 0.450 (- (reduce dfn/max (data color-field))
                                                      (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          full-height
       :width           half-width
       :y-field         y-field
       :y-field-desc    y-desc-field
       :y-field-label   (axis-labels y-field)
       :y-sort-field    y-order-field
       :x-field         x-field
       :x-field-desc    x-desc-field
       :x-field-label   (axis-labels x-field)
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           (format "# EHCPs per Designation by Primary Need in %d" most-recent-year)
       :white-text-test white-text-test}))))

