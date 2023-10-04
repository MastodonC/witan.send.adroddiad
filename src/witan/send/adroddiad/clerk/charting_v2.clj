(ns witan.send.adroddiad.clerk.charting-v2
  (:require
   [clojure2d.color :as color]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [tech.v3.datatype.functional :as dfn]
   [witan.send.adroddiad.dataset :as ds]
   [witan.send.adroddiad.transitions :as tr]))

(def full-height 600)
(def two-rows 200)
(def half-width 600)
(def full-width 1420)

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
    :or {chart-height full-height
         chart-width full-width
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
    :or {chart-height full-height
         chart-width full-width
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

(def field-descriptions
  {:setting         :setting-label
   :setting-1       :setting-1-label
   :setting-2       :setting-2-label
   :need            :need-label
   :need-1          :need-1-label
   :need-2          :need-2-label
   :academic-year   :academic-year-label
   :academic-year-1 :academic-year-1-label
   :academic-year-2 :academic-year-2-label
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
   :scenario-label        "Scenario"
   :scenario              "EHCP Count"})

(def axis-labels
  {:setting "Setting"
   :need "Need"
   :calendar-year "Calendar Year"
   :setting-1 "Setting 1"
   :setting-2 "Setting 2"
   :academic-year-1 "NCY 1"
   :academic-year-2 "NCY 2"
   :academic-year "NCY"})

(def sort-field
  {:setting :setting-order
   :setting-1 :setting-1-order
   :setting-2 :setting-2-order
   :need :need-order
   :need-1 :need-1-order
   :need-2 :need-2-order
   :academic-year :academic-year-order
   :academic-year-1 :academic-year-1-order
   :academic-year-2 :academic-year-2-order})

(def transitions-labels->census-labels
  {:setting-1       :setting
   :setting-2       :setting
   :need-1          :need
   :need-2          :need
   :academic-year-1 :academic-year
   :academic-year-2 :academic-year})

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

(defn ehcp-heatmap-per-year
  [census x-field]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field     "Row Count"
         x-field         x-field
         x-order-field   (sort-field x-field)
         x-label         (field-descriptions x-field)
         y-field         :calendar-year
         data            (-> census
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0)
                             (tc/map-columns x-label [x-field]
                                             (fn [s] s))
                             (tc/order-by [x-label :calendar-year])
                             (tc/add-column :order (range)))
         white-text-test (format "datum['%s'] > %d"
                                 (name color-field)
                                 (int (+ (reduce dfn/min (data color-field))
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))
         ]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          150
       :width           full-width
       :y-field         (sweet-column-names y-field y-field)
       :y-field-desc    (field-descriptions y-field y-field)
       :y-field-label   (axis-labels y-field)
       :x-field         (sweet-column-names x-field x-field)
       :x-field-desc    (field-descriptions x-field x-field)
       :x-field-label   (axis-labels x-field)
       :x-sort-field    x-order-field
       :color-field     color-field
       :title           "# EHCPs for Settings by Year"
       :white-text-test white-text-test
       }))))

(defn ehcps-total-by-year
  [census]
  (-> census
      (tc/add-column :scenario "Baseline")
      (ehcp-heatmap-per-year :scenario)))

(defn ehcps-by-setting-per-year
  [census]
  (ehcp-heatmap-per-year census :setting))

(defn ehcp-by-need-by-year
  [census]
  (ehcp-heatmap-per-year census :need))

(defn ehcps-by-ncy-per-year
  [census]
  (ehcp-heatmap-per-year census :academic-year))

(defn ehcps-yoy-change
  [census domain]
  (let [domain-label (sweet-column-names (field-descriptions domain))
        most-recent-year (reduce dfn/max (:calendar-year census))]
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
                       (tc/map-columns $ domain-label [domain]
                                       (fn [s] s))
                       (tc/order-by $ [domain-label :calendar-year])
                       (tc/rename-columns $ {:diff "Count"})
                       (tc/add-column $ :order (range))
                       (tc/rows $ :as-maps))}
      :title {:text  (str "YoY EHCP Count Change by " (axis-labels domain))
              :fontsize 24}
      :height full-height
      :width half-width
      :encoding {:x {:field "Count" :type "quantitative"}
                 :y {:field domain-label :type "nominal"}
                 :tooltip [{:field domain, :type "nominal", :title domain-label},
                           {:field "Count", :title "Count"}]}
      :mark "bar"})))

(defn ehcps-by-setting-yoy-change
  [census]
  (ehcps-yoy-change census :setting))

(defn ehcps-by-need-yoy-change
  [census]
  (ehcps-yoy-change census :need))

(defn ehcps-by-ncy-yoy-change
  [census]
  (ehcps-yoy-change census :academic-year))

(defn ehcp-yoy-pct-change
  [census domain]
  (let [domain-label (sweet-column-names (field-descriptions domain))
        most-recent-year (reduce dfn/max (:calendar-year census))]
    (clerk/vl {:data {:values (as-> census $
                                (tc/group-by $ [domain :calendar-year])
                                (tc/aggregate $ {:count tc/row-count})
                                (tc/order-by $ [domain :calendar-year])
                                (tc/group-by $ [domain] {:result-type :as-map})
                                (map #(ds/add-diff-and-pct-diff (val %) :count :calendar-year) $)
                                (apply tc/concat $)
                                (tc/replace-missing $ :pct-change)
                                (tc/select-rows $ #(= most-recent-year (:calendar-year %)))
                                (tc/map-columns $ domain-label [domain]
                                                (fn [s] s))
                                (tc/order-by $ [domain-label :calendar-year])
                                (tc/rename-columns $ {:pct-change "% Change"})
                                (tc/add-column $ :order (range))
                                (tc/rows $ :as-maps))}
               :title {:text  (str "EHCP YoY Percentage Change by " (axis-labels domain))
                       :fontsize 24}
               :height full-height
               :width half-width
               :encoding {:x {:field "% Change" :type "quantitative"}
                          :y {:field domain-label :type "nominal"}
                          :tooltip [{:field domain, :type "nominal", :title domain-label},
                                    {:field "% Change", :title "% Change"}]}
               :mark "bar"})))

(defn ehcps-by-setting-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :setting))

(defn ehcps-by-need-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :need))

(defn ehcps-by-ncy-yoy-pct-change
  [census]
  (ehcp-yoy-pct-change census :academic-year))

(defn transitions-heatmap-per-year
  [transitions x-field predicate]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field     "Row Count"
         x-order-field   (sort-field x-field)
         y-field         :calendar-year
         title           (str "# " (cond
                                     (= predicate tr/joiner?)
                                     "Joiners"
                                     (= predicate tr/leaver?)
                                     "Leavers") " by "
                              (sweet-column-names (transitions-labels->census-labels x-field x-field))
                              " by Year")
         ay-order        (-> transitions
                             (tc/unique-by x-field)
                             (tc/order-by x-field)
                             (tc/add-column x-order-field (range))
                             (tc/select-columns [x-field x-order-field]))
         data            (-> transitions
                             (tc/select-rows predicate)
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0)
                             (tc/map-columns :academic-year-label [x-field]
                                             (fn [s] s))
                             (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (inc cy)))
                             (tc/inner-join ay-order [x-field])
                             (tc/order-by [:calendar-year x-field]))
         white-text-test (format "datum['%s'] > %d"
                                 (name color-field)
                                 (int (+ (reduce dfn/min (data color-field))
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))]
     (heatmap-desc
      {:data            (-> data
                            (tc/rows :as-maps))
       :height          two-rows
       :width           full-width
       :y-field         (sweet-column-names y-field y-field)
       :y-field-desc    (field-descriptions y-field y-field)
       :y-field-label   (axis-labels y-field)
       :x-field         (sweet-column-names x-field x-field)
       :x-field-desc    x-field
       :x-field-label   (sweet-column-names (transitions-labels->census-labels x-field x-field))
       :x-sort-field    x-order-field
       :y-sort-field    :calendar-year
       :color-field     color-field
       :title           title
       :white-text-test white-text-test}))))

(defn joiners-by-ehcp-per-year
  [transitions]
  (-> transitions
      (tc/add-column :scenario "Baseline")
      (transitions-heatmap-per-year :scenario witan.send.adroddiad.transitions/joiner?)))

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
  [transitions]
  (-> transitions
      (tc/add-column :scenario "Baseline")
      (transitions-heatmap-per-year :scenario witan.send.adroddiad.transitions/leaver?)))

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
         x-order-field    :setting-2-order
         y-order-field    :setting-1-order
         setting-1-order  (-> transitions
                              (tc/unique-by :setting-1)
                              (tc/order-by :setting-1)
                              (tc/add-column :setting-1-order (range))
                              (tc/select-columns [:setting-1 :setting-1-order]))
         setting-2-order  (-> transitions
                              (tc/unique-by :setting-2)
                              (tc/order-by :setting-2)
                              (tc/add-column :setting-2-order (range))
                              (tc/select-columns [:setting-2 :setting-2-order]))
         data             (-> transitions
                              (tc/select-rows #(= most-recent-year (% :calendar-year)))
                              (tc/group-by [x-field y-field])
                              (tc/aggregate {color-field tc/row-count})
                              (tc/complete x-field y-field)
                              (tc/replace-missing color-field :value 0)
                              (tc/inner-join setting-1-order [y-field])
                              (tc/inner-join setting-2-order [x-field]))
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
       :y-field         (sweet-column-names y-field y-field)
       :y-field-desc    y-field
       :y-field-label   (axis-labels y-field)
       :x-field         (sweet-column-names x-field x-field)
       :x-field-desc    x-field
       :x-field-label   (axis-labels x-field)
       :x-sort-field    x-order-field
       :y-sort-field    y-order-field
       :color-field     color-field
       :title           (str most-recent-year "-" (+ most-recent-year 1) " Setting to Setting Transitions")
       :white-text-test white-text-test}))))

(defn setting-mover-heatmap
  [transitions]
  (clerk/vl
   {::clerk/width :full}
   (let [color-field      "Row Count"
         most-recent-year (reduce dfn/max (:calendar-year transitions))
         y-field          :setting-1
         x-field          :setting-2
         x-order-field    :setting-2-order
         y-order-field    :setting-1-order
         setting-1-order  (-> transitions
                              (tc/unique-by :setting-1)
                              (tc/order-by :setting-1)
                              (tc/add-column :setting-1-order (range))
                              (tc/select-columns [:setting-1 :setting-1-order]))
         setting-2-order  (-> transitions
                              (tc/unique-by :setting-2)
                              (tc/order-by :setting-2)
                              (tc/add-column :setting-2-order (range))
                              (tc/select-columns [:setting-2 :setting-2-order]))
         data             (-> transitions
                              (tc/select-rows #(= most-recent-year (% :calendar-year)))
                              (tc/select-rows tr/mover?)
                              (tc/group-by [x-field y-field])
                              (tc/aggregate {color-field tc/row-count})
                              (tc/complete x-field y-field)
                              (tc/replace-missing color-field :value 0)
                              (tc/inner-join setting-1-order [y-field])
                              (tc/inner-join setting-2-order [x-field]))
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
       :y-field         (sweet-column-names y-field y-field)
       :y-field-desc    y-field
       :y-field-label   (axis-labels y-field)
       :x-field         (sweet-column-names x-field x-field)
       :x-field-desc    x-field
       :x-field-label   (axis-labels x-field)
       :x-sort-field    x-order-field
       :y-sort-field    y-order-field
       :color-field     color-field
       :title           (str most-recent-year "-" (+ most-recent-year 1) " Setting to Setting Movers")
       :white-text-test white-text-test}))))
