(ns witan.send.adroddiad.analysis.setting-to-settings
  (:require
   [tech.v3.datatype.functional :as dfn]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.vega-specs :as vs]))

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

(defn heatmap-desc
  "Creates a map that is a description of a heatmap to pass to clerk/vl"
  [{:keys [title
           x-field x-field-label x-field-desc
           y-field y-field-label y-field-desc
           color-field white-text-test data height width
           color-scheme]
    :or {height 100
         width 550
         color-scheme "viridis"}}]
  (let [tooltip [{:field y-field-desc :type "ordinal" :title y-field-label}
                 {:field x-field-desc :type "ordinal" :title x-field-label}
                 {:field color-field :type "quantitative"}]]
    {:data {:values data}
     :encoding {:x {:field (or x-field-desc x-field) :type "ordinal" :sort x-field :title (or x-field-label x-field)
                    :axis {:labelAngle -45}}
                :y {:field (or y-field-desc y-field) :type "ordinal" :sort y-field :title (or y-field-label y-field)}}
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
                                 :scale {:domainMin 0
                                         :scheme color-scheme}
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

(defn setting-to-setting-heatmap
  [transitions]
  (let [color-field      "Row Count"
        most-recent-year (reduce dfn/max (:calendar-year transitions))
        y-field          :setting-1
        x-field          :setting-2
        data             (-> transitions
                             (tc/select-rows #(= most-recent-year (% :calendar-year)))
                             (tc/group-by [x-field y-field])
                             (tc/aggregate {color-field tc/row-count})
                             (tc/complete x-field y-field)
                             (tc/replace-missing color-field :value 0))
        white-text-test  (format "datum['%s'] < %d"
                                 (name color-field)
                                 (int (+ 0
                                         (* 0.450 (- (reduce dfn/max (data color-field))
                                                     (reduce dfn/min (data color-field)))))))]
    (heatmap-desc
     {:data            (-> data
                           (tc/map-columns color-field [color-field] #(when-not (zero? %) %))
                           (tc/order-by [y-field x-field])
                           (tc/rows :as-maps))
      :height          vs/full-height
      :width           vs/full-width
      :y-field         (sweet-column-names y-field y-field)
      :y-field-desc    y-field
      :y-field-label   (axis-labels y-field)
      :x-field         (sweet-column-names x-field x-field)
      :x-field-desc    x-field
      :x-field-label   (axis-labels x-field)
      :color-field     color-field
      :title           (str most-recent-year "-" (+ most-recent-year 1) " Setting to Setting Transitions")
      :white-text-test white-text-test})))
