^{:nextjournal.clerk/toc true}
(ns historic-analysis
  {:nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.clerk.html :as chtml]
            [witan.send.adroddiad.dataset :as ds]
            [witan.send.adroddiad.clerk.charting-v2 :as chart]
            [witan.send.adroddiad.clerk.slides :as sl]
            [witan.send.adroddiad.slides :as was]
            [clojure.string :as s]))

{:nextjournal.clerk/visibility {:result :hide}}
(
 ;; Template input section
 )

(def client-name nil)
(def sen2-calendar-year nil)
(def date-string nil)
(def out-dir nil)
(def workpackage-name nil)
(def previous-workpackage-name nil)
(def previous-workpackage-config nil)
(def in-dir nil)
(def census (tc/dataset (str in-dir nil) {:key-fn keyword}))
(def transitions (tc/dataset (str in-dir nil) {:key-fn keyword}))
(def population (tc/dataset (str in-dir nil) {:key-fn keyword}))
(def min-year (apply min (:calendar-year transitions)))

(
 ;; Supporting functions and defs
 )

(clerk/add-viewers! [slideshow/viewer])

(comment
  ;; Writes out to to a standalone html file
  (chtml/ns->html out-dir *ns*)

  )

(
;;; Charting helpers
 )

(def full-height 600)
(def two-rows 200)
(def half-width 600)
(def full-width 1420)

(def chart-base
  {:data              nil
   :chart-height      full-height    :chart-width full-width
   :clerk-width       :full
   :chart-title       nil
   :range-format-f    (fn [lower upper]
                        (format "%,.2f - %,.2f" lower upper))
   :x                 :calendar-year :x-title     "Year"    :x-format "%Y"
   :y                 :median        :y-title     "# EHCPs" :y-zero true
   :irl               :q1            :iru         :q3       :ir-title "50% range"
   :orl               :p05           :oru         :p95      :or-title "90% range"
   :group             :baseline      :group-title nil
   :colors-and-shapes nil})

(def setting-rules
  (mapv (fn [[setting simple-setting]] (conj [#(s/includes? % setting) simple-setting]))
        [["6FC" "Further Education"]
         ["APRU" "APRU"]
         ["EHE" "Other"]
         ["EYP" "Early Years"]
         ["EYS" "Early Years"]
         ["MsMdA" "Mainstream"]
         ["MsIn" "Independent"]
         ["SENU" "Resource Provision/Units"]
         ["RP" "Resource Provision/Units"]
         ["SpMdA" "Maintained Special"]
         ["SpNm" "NMI"]
         ["SpIn" "NMI"]
         ["GFE" "Further Education"]
         ["SP16" "Specialist Post-16"]
         ["OLAS" "Other"]
         ["OPA" "Other"]
         ["NEET" "Not in education"]
         ["NIEC" "Not in education"]
         ["NIEO" "Not in education"]
         ["UKN" "Other"]]))

(defn classify
  ([record rules]
   (or (some (fn [[pred result]] (when (pred record) result))
             rules)
       record))
  ([record]
   (classify record setting-rules)))

(defn phase [ncy]
  (cond
    ((into (sorted-set) (range -5 1)) ncy)
    "Under 5"
    ((into (sorted-set) (range 1 7)) ncy)
    "Primary"
    ((into (sorted-set) (range 7 12)) ncy)
    "Secondary"
    ((into (sorted-set) (range 12 15)) ncy)
    "16 to 19"
    ((into (sorted-set) (range 15 21)) ncy)
    "19+"))

(def phase-order
  {"Under 5" 1
   "Primary" 2
   "Secondary" 3
   "16 to 19" 4
   "19+" 5})

(
 ;; Data
 )

(def simple-settings-census
  (-> census
      (tc/map-columns :setting [:setting]
                      (fn [s] (classify s)))))

(def total-historic-ehcps
  (let [min-year (apply min (:calendar-year transitions))]
    (-> transitions
        (tc/add-column :dataset (str min-year " to " anchor-year " SEN2"))
        (tr/transitions->census)
        (tc/group-by [:dataset :calendar-year])
        (tc/aggregate {:median #(-> % tc/row-count double)})
        (tc/rename-columns {:$group-name :calendar-year}))))

(def simple-settings-transitions
  (-> transitions
      (tc/map-columns :setting-1 [:setting-1]
                      (fn [s] (classify s)))
      (tc/map-columns :setting-2 [:setting-2]
                      (fn [s] (classify s)))))

(def transitions-w-joiner-leaver-labels
  (-> transitions-w-simple-settings
      (tc/map-columns :setting-1 [:setting-1]
                      (fn [s] ({"NONSEND" "Joiner"} s s)))
      (tc/map-columns :setting-2 [:setting-2]
                      (fn [s] ({"NONSEND" "Leaver"} s s)))))

(def previous-baseline-summaries
  (let [year (- sen2-year 1)]
    (tep/summary-charts-and-data-from-config {:config-edn previous-workpackage-config
                                              :pqt-prefix previous-workpackage-name
                                              :anchor-year 2025
                                              :colors-and-shapes (chart/color-and-shape-lookup ["2025 Baseline"])
                                              :projection "2025 Baseline"})))

(def summarised-send-age-pop-data
  (-> (tc/dataset population {:key-fn keyword})
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population #(dfn/sum (:population %))})))

(def summarised-school-age-pop-data
  (-> (tc/dataset population {:key-fn keyword})
      (tc/select-rows #((into (sorted-set) (range 0 12)) (:academic-year %)))
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population #(dfn/sum (:population %))})))

(def total-ehcp-summary
  (let [summarise (-> transitions
                      (tr/transitions->census)
                      (tc/group-by [:calendar-year])
                      (tc/aggregate {:transition-count #(-> % tc/row-count double)})
                      (tep/add-diff :transition-count :calendar-year)
                      (tc/rename-columns
                       {:diff :ehcp-diff
                        :pct-diff :ehcp-pct-diff})
                      (tc/inner-join summarised-send-age-pop-data [:calendar-year])
                      (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
                      (tc/add-column :dataset "SEN2"))]
    (-> summarise
        (tc/order-by [:calendar-year]))))

(def school-age-ehcp-summary
  (-> transitions
      (tr/transitions->census)
      (tc/select-rows #((into (sorted-set) (range 0 12)) (:academic-year %)))
      (tc/group-by [:calendar-year])
      (tc/aggregate {:transition-count #(-> % tc/row-count double)})
      (tep/add-diff :transition-count :calendar-year)
      (tc/rename-columns
       {:diff :ehcp-diff
        :pct-diff :ehcp-pct-diff})
      (tc/inner-join summarised-school-age-pop-data [:calendar-year])
      (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
      (tc/add-column :dataset "SEN2")
      (tc/order-by [:calendar-year])))

(
 ;; Slides
 )

;; ---
(def title-content
  {:title (format "%1s Historical Analysis for %2s" workpackage-name sen2-calendar-year)
   :work-package workpackage-name
   :presentation-date date-string
   :client-name (format "For %s" client-name)
   :slide-type ::was/title-slide})

(def agenda-content
  {:title "Agenda"
   :text ["TBC"]
   :slide-type ::was/title-body-slide})

(def summary
  {:title "Summary"
   :text ["Key takeaway 1"
          "Key takeaway 2"
          "Key takeaway 3"]
   :slide-type ::was/title-body-slide})

(def historic-total-ehcp-growth
  {:title "TBD"
   :chart {:hconcat [(-> census
                         chart/ehcps-total-by-year
                         (assoc-in [:nextjournal/value :encoding :x :axis :labelAngle] 0))
                     (chart/echps-total-yoy-change census)
                     (chart/echps-total-yoy-pct-change census)]}
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def total-ehcp-line-chart
  {:title "TBD"
   :chart (vsl/line-plot
           (merge chart-base
                  {:data         (-> total-historic-ehcps
                                     (tc/order-by [:dataset :calendar-year])
                                     (tc/map-columns :calendar-year [:calendar-year] str))
                   :chart-title  "Total EHCP Counts by Calendar Year"
                   :chart-height 300
                   :colors-and-shapes (chart/color-and-shape-lookup [(str min-year " to " sen2-calendar-year" SEN2")])
                   :group        :dataset   :group-title "Dataset"
                   :x-axis-title-size 16 :x-axis-font-size 16
                   :y-axis-title-size 16 :y-axis-font-size 16}))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def comparison-to-previous-baseline
  {:title "TBD"
   :chart (let [data (-> total-historic-ehcps
                         (tc/concat (-> previous-baseline-summaries
                                        :transition-count-summary
                                        :table
                                        (tc/add-column :dataset (str (- sen2-calendar-year 1) " Baseline"))
                                        (tc/select-rows #(>= sen2-calendar-year (:calendar-year %)))))
                         (tc/order-by [:dataset :calendar-year])
                         (tc/map-columns :calendar-year [:calendar-year] str))]
            (vsl/line-shape-and-ribbon-plot
             (-> chart-base
                 (merge {:data         data
                         :chart-title  (str sen2-calendar-year " Data Vs Previous Census & Projected Counts")
                         :chart-height 300
                         :colors-and-shapes (chart/color-and-shape-lookup [(str min-year " to " sen2-calendar-year " SEN2")
                                                                           (str (- sen2-calendar-year 1) " Baseline")])
                         :group        :dataset   :group-title "Dataset" :legend true}))))
   :text []
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def ehcp-rates
  {:title "TBD"
   :chart {:hconcat [(vsl/line-plot
                      (merge chart-base
                             {:data (tc/concat (-> total-ehcp-summary
                                                   (tc/map-columns :pct-ehcps #(* % 100))
                                                   (tc/add-column :dataset "0-25 aged population")
                                                   (tc/map-columns :calendar-year [:calendar-year] str))
                                               (-> school-age-ehcp-summary
                                                   (tc/map-columns :pct-ehcps #(* % 100))
                                                   (tc/add-column :dataset "School age population")
                                                   (tc/map-columns :calendar-year [:calendar-year] str)))
                              :y :pct-ehcps
                              :y-title "% of population"
                              :chart-title "EHCPs as percentage of population"
                              :chart-height 300
                              :chart-width 250
                              :colors-and-shapes (chart/color-and-shape-lookup ["0-25 aged population"
                                                                                "School age population"])
                              :group :dataset :group-title "Dataset"
                              :x-axis-title-size 16 :x-axis-font-size 16
                              :y-axis-title-size 16 :y-axis-font-size 16}))
                     (vsl/line-plot
                      (merge chart-base
                             {:data (tc/concat (-> total-ehcp-summary
                                                   (tc/add-column :dataset "0-25 aged population")
                                                   (tc/map-columns :calendar-year [:calendar-year] str))
                                               (-> school-age-ehcp-summary
                                                   (tc/add-column :dataset "School age population")
                                                   (tc/map-columns :calendar-year [:calendar-year] str)))
                              :y :population
                              :y-title "Population"
                              :chart-title "0-25 aged population"
                              :chart-height 300
                              :chart-width 250
                              :colors-and-shapes (chart/color-and-shape-lookup ["0-25 aged population"
                                                                                "School age population"])
                              :group :dataset :group-title "Dataset"
                              :x-axis-title-size 16 :x-axis-font-size 16
                              :y-axis-title-size 16 :y-axis-font-size 16}))]
           :config {:legend {:titleFontSize 20 :labelFontSize 14 :labelLimit 0}}}
   :text [(clerk/html
           [:div.text-1xl.max-w-screen-2xl.font-sans
            (reduce #(into %1 [[:li.text-3xl.mb-4.mt-4 %2]]) [:ul.list-disc]
                    [(str (as-> school-age-ehcp-summary $
                            (tc/select-rows $ #(#{sen2-calendar-year} (:calendar-year %)))
                            (:pct-ehcps $)
                            (first $)
                            (* $ 100)
                            (float $)
                            (format "%,.1f" $))
                          "% of school aged CYPs have an EHCP")
                     (str "Currently "
                          (as-> total-ehcp-summary $
                            (tc/select-rows $ #(#{sen2-calendar-year} (:calendar-year %)))
                            (:pct-ehcps $)
                            (first $)
                            (* $ 100)
                            (float $)
                            (format "%,.1f" $))
                          "% of eligible population with an EHCP (0-25 year olds)")])])]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def settings-heatmap
  {:title "TBD"
   :chart (-> simple-settings-census
              chart/ehcps-by-setting-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 45))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def setting-rates
  {:title "TBD"
   :chart {:hconcat [(-> simple-settings-census
                         chart/ehcps-by-setting-yoy-change)
                     (-> simple-settings-census
                         chart/ehcps-by-setting-yoy-pct-change)]}
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def needs-heatmap
  {:title "TBD"
   :chart (-> census
              chart/ehcp-by-need-by-year
              (assoc-in [:encoding :x :axis :labelAngle] 45))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def need-rates
  {:title  "TBD"
   :chart {:hconcat [(-> census
                         chart/ehcps-by-need-yoy-change)
                     (-> census
                         chart/ehcps-by-need-yoy-pct-change)]}
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def ncy-heatmap
  {:title "TBD"
   :chart (-> census
              chart/ehcps-by-ncy-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 0))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def ncy-rates
  {:title  "TBD"
   :chart {:hconcat [(chart/ehcps-by-ncy-yoy-change census)
                     (chart/ehcps-by-ncy-yoy-pct-change census)]}
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def joiner-counts
  {:title "New EHCPs are down for the first time"
   :chart (-> transitions
              chart/joiners-by-ehcp-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 0))
   :text []
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def joiner-heatmap-by-ncy
  {:title "TBD"
   :chart (-> transitions
              chart/joiners-by-ncy-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 0))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def joiner-heatmap-by-need
  {:title "TBD"
   :chart (-> transitions
              chart/joiners-by-need-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 45))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def joiner-heatmap-by-setting
  {:title "TBD"
   :chart (-> transitions-w-simple-settings
              chart/joiners-by-setting-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 45))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def leaver-counts
  {:title "TBD"
   :chart (-> transitions
              chart/leavers-by-ehcp-per-year
              (assoc-in [:encoding :x :axis :titleFontSize] 20))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def leaver-heatmap-by-ncy
  {:title "TBD"
   :chart (-> transitions
              chart/leavers-by-ncy-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 0))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def leaver-heatmap-by-setting
  {:title "TBD"
   :chart (-> transitions-w-simple-settings
              chart/leavers-by-setting-per-year
              (assoc-in [:encoding :x :axis :labelAngle] 45))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def setting-to-setting-transitions
  {:title "TBD"
   :chart (-> transitions-w-joiner-leaver-labels
              chart/setting-to-setting-heatmap
              (assoc-in [:encoding :x :axis :labelAngle] 45)
              (assoc :width 1000))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def mover-transitions
  {:title "TBD"
   :chart (-> transitions-w-simple-settings
              chart/setting-mover-heatmap
              (assoc-in [:encoding :x :axis :labelAngle] 45)
              (assoc :width 1000))
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def transitions-by-setting-and-ncy
  {:title "There are hotspots of new EHCPs around Key Stage transfers, but not as pronounced as might be expected"
   :chart (-> transitions-w-simple-settings
              chart/joiners-by-setting-and-ncy
              (assoc-in [:encoding :x :axis :labelAngle] 0)
              (assoc :width 1000))
   :text []
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def needs-by-designation
  {:title "TBD"
   :chart (chart/needs-by-designation census)
   :text ["TBD"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def conclusions
  {:title "Conclusions"
   :text  ["TBC"]
   :slide-type ::was/title-body-slide})

(def next-steps
  {:title "Next steps"
   :text ["TBC"]
   :slide-type ::was/title-body-slide})

{:nextjournal.clerk/visibility {:result :show}}

(
 ;; Notebook
 )

(sl/slide title-content)

;; ---

(sl/slide agenda-content)

;; ---

(sl/slide summary)

;; ---

(sl/slide historic-total-ehcp-growth)

;; ---

(sl/slide total-ehcp-line-chart)

;; ---

(sl/slide comparison-to-previous-baseline)

;; ---

(sl/slide ehcp-rates)

;; ---

(sl/slide settings-heatmap)

;; ---

(sl/slide setting-rates)

;; ---

(sl/slide needs-heatmap)

;; ---

(sl/slide need-rates)

;; ---

(sl/slide ncy-heatmap)

;; ---

(sl/slide ncy-rates)

;; ---

(sl/slide joiner-counts)

;; ---

(sl/slide joiner-heatmap-by-ncy)

;; ---

(sl/slide joiner-heatmap-by-need)

;; ---

(sl/slide joiner-heatmap-by-setting)

;; ---

(sl/slide leaver-counts)

;; ---

(sl/slide leaver-heatmap-by-ncy)

;; ---

(sl/slide leaver-heatmap-by-setting)

;; ---

(sl/slide setting-to-setting-transitions)

;; ---

(sl/slide mover-transitions)

;; ---

(sl/slide transitions-by-setting-and-ncy)

;; ---

(sl/slide needs-by-designation)

;; ---

(sl/slide conclusions)

;; ---

(sl/slide next-steps)
