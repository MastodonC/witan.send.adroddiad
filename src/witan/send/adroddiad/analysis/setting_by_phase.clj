(ns witan.send.adroddiad.analysis.setting-by-phase
  (:require
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.analysis.total-domain :as td]
   [witan.send.domain.academic-years :as day]))

(def setting-phase-summary-defaults
  {:domain-key [:phase :setting]
   :extra-transformation-f
   (fn [ds]
     (tc/map-columns ds :phase [:academic-year] day/ncy->school-phase-name))})

(defn summarise-from-config
  "This needs the path to a config.edn and a parquet prefix for the results."
  [config]
  (-> (td/summarise-from-config
       (merge config setting-phase-summary-defaults))
      (update-vals
       (fn [m]
         (if-let [settings-lookup (:settings-lookup config)]
           (assoc m
                  :table
                  (tc/left-join (:table m) settings-lookup [:setting]))
           m)))))

(def color-shapes-order
  (-> ["Nursery" "Primary" "Secondary" "Post 16" "Post 19"]
      (vs/color-and-shape-lookup)
      (tc/add-column :order (range))))

(
;;; Charting
 )
(def base-chart-spec
  {:y   :median
   :irl :q1  :iru :q3  :ir-title "50% range"
   :orl :p05 :oru :p95 :or-title "90% range"})

(defn line-and-ribbon-and-rule-plot
  [{:keys [data group group-title colors-and-shapes order-field
           x x-title x-scale
           y y-title y-format y-zero
           chart-title chart-width chart-height]
    :as chart-spec}]
  (vsl/line-and-ribbon-and-rule-plot
   (merge base-chart-spec chart-spec)))

(defn format-calendar-year
  [d]
  ;; (str d "-01-01")
  (str d))


(defn total-summary-plot
  ([summary-data setting config]
   (let [label-field :phase
         data (-> summary-data
                  (tc/select-rows #(= setting (:setting %)))
                  (tc/inner-join color-shapes-order [:phase :domain-value])
                  (tc/map-columns :calendar-year [:calendar-year] format-calendar-year))
         title-field (if (seq (:setting-label summary-data)) :setting-label :setting)]
     (line-and-ribbon-and-rule-plot
      (merge
       {:data              data
        :chart-title       (format "# CYP in %s by %s" (-> data title-field first) (name label-field))
        :chart-height      vs/full-height :chart-width vs/two-thirds-width
        :tooltip-formatf   (vsl/number-summary-tooltip {:group label-field :x :calendar-year :tooltip-field :tooltip-column})
        :colors-and-shapes color-shapes-order
        :x                 :calendar-year :x-title     "Census Year" :x-format    "%b %Y"
        :y-title           "# EHCPs"      :y-zero      true          :y-scale     false
        :group             label-field    :group-title "Phase"       :order-field :order}
       config))))
  ([summary-data setting]
   (total-summary-plot summary-data setting {})))

#_
(defn diff-summary-plot
  [{}]
  )

(defn pct-diff-summary-plot
  [summary-data setting]
  (let [label-field :phase
        order-field :order]
    (line-and-ribbon-and-rule-plot
     {:data              (-> summary-data
                             (tc/select-rows #(= setting (:setting %)))
                             (tc/inner-join color-shapes-order [:phase :domain-value])
                             (tc/map-columns :calendar-year [:calendar-year] format-calendar-year))
      :chart-title       (format "% change in %s by %s" setting (name label-field))
      :chart-height      vs/full-height :chart-width vs/two-thirds-width
      :tooltip-formatf   (vsl/pct-summary-tooltip {:group label-field :x :calendar-year :tooltip-field :tooltip-column})
      :colors-and-shapes color-shapes-order
      :x                 :calendar-year :x-title     "Census Year" :x-format    "%b %Y"
      :y-title           "% change"     :y-zero      true          :y-scale     false :y-format ".1%"
      :group             label-field    :group-title "Setting"     :order-field order-field})))

#_
(defn pct-of-total-summary-plot
  [{}]
  )

(
;;; Each Setting on its own
 )
