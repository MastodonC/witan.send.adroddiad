^{:nextjournal.clerk/toc true}
(ns baseline-template
  {:nextjournal.clerk/visibility {:code   :hide
                                  :result :hide}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send :as ws]
            [witan.send.domain.academic-years :as day]
            [witan.send.adroddiad.clerk.charting-v2 :as acc]
            [witan.send.adroddiad.clerk.slides :as sl]
            [witan.send.adroddiad.clerk.html :as chtml]
            [witan.send.adroddiad.slides :as was]
            [witan.send.adroddiad.pptx.slides :as pptx]
            [witan.send.adroddiad.analysis.total-ehcp-projection :as tep]
            [witan.send.adroddiad.analysis.total-domain :as td]
            [witan.send.adroddiad.vega-specs.lines :as vsl]
            [witan.send.adroddiad.transitions :as tr]
            [witan.send.foo.wp-n-n.census :as c] ;; replace with census namespace
            [witan.send.foo.wp-n-n.groups :as g] ;; replace with groups namespace
            [clojure.java.io :as io]))

(
 ;; Notebook defaults
 )

(clerk/add-viewers! [slideshow/viewer])


(comment

  (chtml/ns->html out-dir *ns*)

  )

(
;;; Template input section
 )

(def wp "wp-4-2")

(def out-dir (str wp "/"))

(def config-file (str out-dir "config.edn"))

(def config (ws/read-config config-file))

(def anchor-year 2026)

(def previous-baseline-wp "wp-3-2")

(def previous-out-dir (str previous-baseline-wp "/"))

(def previous-config-file (str "../???" previous-out-dir "config.edn"))

(def previous-anchor-year (- anchor-year 1))

(def historic-transition-counts
  (-> config-file
      td/transitions-from-config
      td/historic-ehcp-count))

(def oldest-calendar-year
  (apply min (:calendar-year historic-transition-counts)))

(def presentation-title nil)
(def presentation-date nil)
(def client-name nil)
(def work-package nil)

(
;;; Helper fns
 )

(defn %-change [diff initial-count]
  (read-string (format "%.1f" (float (* 100 (/ diff (tc/row-count initial-count)))))))

(def full-height 500)
(def full-width 800)

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
   :colors-and-shapes nil
   :x-axis-title-size 20 :x-axis-font-size 18
   :y-axis-title-size 20 :y-axis-font-size 18})

(defn transform-setting-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :setting [:setting]
                      (fn [s] (g/simple-settings s s)))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :setting {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(defn transform-phase-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :phase [:academic-year]
                      (fn [ncy] ((merge day/school-phase-names {:early-years "Early Years"
                                                                :outside-of-send-age "Outside of SEND age"})
                                 (day/primary-secondary-post16-ncy15+ ncy))))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :phase {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(
;;; Data
 )

(def census-2022 (tc/select-rows @c/census-out #(= 2022 (:calendar-year %))))
(def census-2023 (tc/select-rows @c/census-out #(= 2023 (:calendar-year %))))
(def census-2024 (tc/select-rows @c/census-out #(= 2024 (:calendar-year %))))
(def census-2025 (tc/select-rows @c/census-out #(= 2025 (:calendar-year %))))
(def census-2026 (tc/select-rows @c/census-out #(= 2026 (:calendar-year %))))

(def census-diff-22-23 (- (tc/row-count census-2023)
                          (tc/row-count census-2022)))

(def census-diff-23-24 (- (tc/row-count census-2024)
                          (tc/row-count census-2023)))

(def census-diff-24-25 (- (tc/row-count census-2025)
                          (tc/row-count census-2024)))

(def census-diff-25-26 (- (tc/row-count census-2026)
                          (tc/row-count census-2025)))

(def summarised-pop-data
  (-> (tc/dataset (str out-dir "population.csv") {:key-fn keyword})
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population #(dfn/sum (:population %))})))

(def summarised-school-age-pop-data
  (-> (tc/dataset (str out-dir "population.csv") {:key-fn keyword})
      (tc/select-rows #((into (sorted-set) (range 0 12)) (:academic-year %)))
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population #(dfn/sum (:population %))})))



(def school-age-ehcp-summary
  (-> historic-transition-counts
      (tr/transitions->census)
      (tc/select-rows #((into (sorted-set) (range 0 12)) (:academic-year %)))
      (tc/group-by [:calendar-year])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (tep/add-diff :transition-count :calendar-year)
      (tc/rename-columns
       {:diff :ehcp-diff
        :pct-diff :ehcp-pct-diff})
      (tc/inner-join summarised-school-age-pop-data [:calendar-year])
      (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
      (tc/add-column :dataset "SEN2")
      (tc/order-by [:calendar-year])))

(def total-historic-ehcps
  (-> historic-transition-counts
      (tc/add-column :dataset (str oldest-calendar-year " to " anchor-year " SEN2"))
      (tr/transitions->census)
      (tc/group-by [:dataset :calendar-year])
      (tc/aggregate {:median #(dfn/sum (:transition-count %))})
      (tc/rename-columns {:$group-name :calendar-year})
      (tc/add-column :min 0.0)
      (tc/add-column :p05 0.0)
      (tc/add-column :q1 0.0)
      (tc/add-column :q3 0.0)
      (tc/add-column :p95 0.0)
      (tc/add-column :max 0.0)
      (tc/add-column :observations 1000)))

(def total-ehcp-summary
  (let [summarise (-> historic-transition-counts
                      (tr/transitions->census)
                      (tc/group-by [:calendar-year])
                      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                      (tep/add-diff :transition-count :calendar-year)
                      (tc/rename-columns
                       {:diff :ehcp-diff
                        :pct-diff :ehcp-pct-diff})
                      (tc/inner-join summarised-pop-data [:calendar-year])
                      (tc/map-columns :pct-ehcps [:transition-count :population] #(dfn// %1 %2))
                      (tc/add-column :dataset "SEN2"))]
    (-> summarise
        (tc/order-by [:calendar-year]))))

(def total-summaries (tep/summary-charts-and-data-from-config {:config-edn config-file
                                                               :pqt-prefix wp
                                                               :anchor-year anchor-year
                                                               :colors-and-shapes (acc/color-and-shape-lookup [(str anchor-year " Baseline")])
                                                               :projection (str anchor-year " Baseline")}))

(def previous-total-summaries
  (tep/summary-charts-and-data-from-config {:config-edn ;; previous-config-file
                                            "../witan.send.south-glos.wp-3-2/wp-3-2/wp-3-2-1/config.edn"
                                            :pqt-prefix ;; previous-baseline-wp
                                            "simulations"
                                            :anchor-year previous-anchor-year
                                            :colors-and-shapes (acc/color-and-shape-lookup [(str previous-anchor-year " Baseline")])
                                            :projection (str previous-anchor-year " Baseline")}))

(def simulation-data (td/simulation-data-from-config config-file wp))

(def simulation-count (get-in config [:projection-parameters :simulations]))

(def setting-summaries (td/summarise simulation-data
                                     {:domain-key :setting
                                      :historic-transitions-count historic-transition-counts
                                      :simulation-count simulation-count
                                      :transform-simulation-f transform-setting-simulation}))

(def previous-setting-summaries
  (let [cfg (ws/read-config ;; previous-config-file
             "../witan.send.south-glos.wp-3-2/wp-3-2/wp-3-2-1/config.edn")]
    (td/summarise (td/simulation-data-from-config ;; previous-config-file
                   "../witan.send.south-glos.wp-3-2/wp-3-2/wp-3-2-1/config.edn" ;; previous-baseline-wp
                   "simulations")
                  {:domain-key :setting
                   :historic-transitions-count (-> ;; previous-config-file
                                                "../witan.send.south-glos.wp-3-2/wp-3-2/wp-3-2-1/config.edn"
                                                td/transitions-from-config
                                                td/historic-ehcp-count)
                   :simulation-count (get-in cfg [:projection-parameters :simulations])
                   :transform-simulation-f transform-setting-simulation})))

(def setting-colours (acc/color-and-shape-lookup (into (sorted-set) (:setting (get-in setting-summaries [:total-summary :table])))))

(def need-summaries (->> (td/summarise simulation-data
                                       {:domain-key :need
                                        :historic-transitions-count historic-transition-counts
                                        :simulation-count simulation-count})))

(def need-colours (acc/color-and-shape-lookup (into (sorted-set) (-> need-summaries
                                                                     (get-in [:total-summary :table])
                                                                     :need))))

(def phase-summaries (td/summarise simulation-data
                                   {:domain-key :phase
                                    :historic-transitions-count historic-transition-counts
                                    :simulation-count simulation-count
                                    :transform-simulation-f transform-phase-simulation}))

(def ncy-summaries (td/summarise simulation-data
                                 {:domain-key :academic-year
                                  :historic-transitions-count historic-transition-counts
                                  :simulation-count simulation-count}))

(def ncy-colours (acc/color-and-shape-lookup (into (sorted-set) (:academic-year (get-in ncy-summaries [:total-summary :table])))))

(
 ;;; Slides
 )

(def title-content
  {:title presentation-title
   :work-package work-package
   :presentation-date presentation-date
   :client-name client-name
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

(def baseline-description
  {:title "\"baseline projection\" - assumes everything remains the same"
   :slide-type ::was/section-header-slide})

(def modelling
  {:title "How the SEND model works"
   :image (io/resource "model-diagram.png")
   :slide-type ::was/title-body-slide})

(def historic-total-ehcp-growth
  {:title "TBC"
   :chart {:hconcat [(-> (merge chart-base
                                {:data         (-> total-historic-ehcps
                                                   (tc/order-by [:dataset :calendar-year])
                                                   (tc/map-columns :calendar-year [:calendar-year] str))
                                 :chart-title  "Total EHCP Counts by Calendar Year"
                                 :chart-height 300
                                 :colors-and-shapes (acc/color-and-shape-lookup [(str oldest-calendar-year " to " anchor-year " SEN2")])
                                 :group        :dataset   :group-title "Dataset"})
                         vsl/line-shape-and-ribbon-plot)
                     (-> {:data {:values (as-> total-ehcp-summary $
                                           (tc/order-by $ [:calendar-year])
                                           (tc/map-columns $ :ehcp-pct-diff #(format "%,.1f" (* 100 %)))
                                           (tc/rename-columns $ {:ehcp-pct-diff "% Change"})
                                           (tc/replace-missing $ "% Change" :value 0)
                                           (tc/add-column $ :order (range))
                                           (tc/drop-rows $ #(= oldest-calendar-year (:calendar-year %)))
                                           (tc/rows $ :as-maps))}
                          :title {:text "EHCP YoY Percentage Change"
                                  :fontsize 24}
                          :height 300
                          :width 200
                          :encoding {:x {:field "% Change" :type "quantitative"}
                                     :y {:field :calendar-year :type "nominal" :title "Calendar Year"}
                                     :tooltip [{:field :calendar-year, :type "nominal", :title "Year"},
                                               {:field "% Change", :title "% Change"}]}
                          :mark "bar"})]}
   :text (let [current-census (tc/select-rows @c/census-out #(= anchor-year (:calendar-year %)))
               previous-census (tc/select-rows @c/census-out #(= previous-anchor-year (:calendar-year %)))
               current-census-diff (- (tc/row-count current-census) (tc/row-count previous-census))]
           [(str "There were "
                 (-> @c/census-out
                     (tc/select-rows #(= anchor-year (:calendar-year %)))
                     tc/row-count)
                 " EHCPs recorded in Jan "
                 anchor-year)
            (str "This is increase of " current-census-diff " EHCPs from " previous-anchor-year " or up "
                 (%-change current-census-diff previous-census) "%")])
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def comparison-of-projection-vs-current-counts
  (let [data (-> total-historic-ehcps
                 (tc/concat (-> previous-total-summaries
                                :transition-count-summary
                                :table
                                (tc/add-column :dataset (str previous-anchor-year " Baseline"))
                                (tc/select-rows #(>= anchor-year (:calendar-year %)))))
                 (tc/order-by [:dataset :calendar-year]))
        previous-dataset-name (str previous-anchor-year " Baseline")
        current-dataset-name (str oldest-calendar-year " to " anchor-year " SEN2")
        previous-anchor-year-count (-> data
                                       (tc/select-rows #(and (= previous-anchor-year (:calendar-year %))
                                                             (= previous-dataset-name (:dataset %))))
                                       :median
                                       first
                                       int)
        current-previous-anchor-year-count (-> data
                                               (tc/select-rows #(and (= previous-anchor-year (:calendar-year %))
                                                                     (= current-dataset-name (:dataset %))))
                                               :median
                                               first
                                               int)
        previous-anchor-year-delta (- current-previous-anchor-year-count previous-anchor-year-count)
        projected-anchor-year-count (-> data
                                        (tc/select-rows #(and (= anchor-year (:calendar-year %))
                                                              (= previous-dataset-name (:dataset %))))
                                        :median
                                        first
                                        int)
        current-anchor-year-count (-> data
                                      (tc/select-rows #(and (= anchor-year (:calendar-year %))
                                                            (= current-dataset-name (:dataset %))))
                                      :median
                                      first
                                      int)]
    {:title "TBC"
     :chart (vsl/line-shape-and-ribbon-plot
             (-> chart-base
                 (merge {:data         (tc/map-columns data :calendar-year str)
                         :chart-title  (str anchor-year " Data Vs Previous Census & Projected Counts")
                         :chart-height 300
                         :colors-and-shapes (acc/color-and-shape-lookup [current-dataset-name previous-dataset-name])
                         :group        :dataset   :group-title "Dataset" :legend true})))
     :text [(str previous-anchor-year
                 "'s counts have increased from "
                 previous-anchor-year-count
                 " to "
                 current-previous-anchor-year-count
                 ", a difference of "
                 previous-anchor-year-delta)
            (str "In Jan " anchor-year " there were "
                 current-anchor-year-count
                 " ECHPs, a difference of "
                 (- current-anchor-year-count projected-anchor-year-count)
                 " than what was previously projected for "
                 anchor-year)]
     :slide-type ::was/title-two-columns-slide
     :left-box ::was/chart
     :right-box ::was/text}))

(def echp-rates
  {:title "TBC"
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
                              :colors-and-shapes (acc/color-and-shape-lookup ["0-25 aged population"
                                                                              "School age population"])
                              :group :dataset :group-title "Dataset"}))
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
                              :colors-and-shapes (acc/color-and-shape-lookup ["0-25 aged population"
                                                                              "School age population"])
                              :group :dataset :group-title "Dataset"}))]}
   :text [(str "Currently "
               (as-> total-ehcp-summary $
                 (tc/select-rows $ #(= anchor-year (:calendar-year %)))
                 (:pct-ehcps $)
                 (first $)
                 (* $ 100)
                 (float $)
                 (format "%,.1f" $))
               "% of eligible population (0-25 year olds) have an EHCP")
          #_(str "This is up from "
                 (as-> total-ehcp-summary $
                   (tc/select-rows $ #(= previous-anchor-year (:calendar-year %)))
                   (:pct-ehcps $)
                   (first $)
                   (* $ 100)
                   (float $)
                   (format "%,.1f" $))
                 "% of eligible population (0-25 year olds) have an EHCP")
          [:p (str (as-> school-age-ehcp-summary $
                     (tc/select-rows $ #(= anchor-year (:calendar-year %)))
                     (:pct-ehcps $)
                     (first $)
                     (* $ 100)
                     (float $)
                     (format "%,.1f" $))
                   "% of school aged CYPs have an EHCP, which is TBC when compared to the national average (")
           [:a {:href "https://lginform.local.gov.uk/reports/view/send-research/local-area-send-report?mod-area=E06000025&mod-group=AllUnitaryLaInCountry_England&mod-type=namedComparisonGroup"
                :target "_blank"
                :class "text-blue-600 underline hover:text-blue-800"} "LG Inform"] ")"]
          (str "This is up from "
               (as-> school-age-ehcp-summary $
                 (tc/select-rows $ #(= previous-anchor-year (:calendar-year %)))
                 (:pct-ehcps $)
                 (first $)
                 (* $ 100)
                 (float $)
                 (format "%,.1f" $))
               "% last year")]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def overall-ehcp-projection
  {:title (first (get-in total-summaries [:transition-count-summary :summary-description]))
   :chart {:vconcat [(merge (get-in total-summaries [:transition-count-summary :plot]) {:height 100})
                     (merge (get-in total-summaries [:ehcp-pct-diff-summary :plot]) {:height 100})
                     (merge (get-in total-summaries [:pct-ehcps-summary :plot]) {:height 100})]}
   :table (-> total-summaries
              (get-in [:echp-diff-summary :table])
              (tc/drop-rows #(= oldest-calendar-year (:calendar-year %)))
              (tc/set-dataset-name "Total EHCP Change Year on Year")
              (tc/select-columns [:calendar-year :median])
              (tc/rename-columns {:calendar-year "Calendar Year"
                                  :median "Actual/Median Difference YoY"}))
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/table})

(def settings-projection
  {:title "TBC"
   :chart (let [data (get-in setting-summaries [:total-summary :table])]
            (-> (td/total-summary-plot {:data data
                                        :colors-and-shapes setting-colours
                                        :order-field :setting
                                        :label-field :setting
                                        :group-title "Setting"
                                        :x-axis-font-size 16
                                        :x-axis-title-size 16
                                        :y-axis-title-size 16
                                        :y-axis-font-size 16
                                        :chart-title "Actual/Projected Count of EHCPs per Setting"})))
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(defn map-net-diff [ds col]
  (let [right-column (keyword (str "right." (name col)))]
    (-> ds
        (tc/map-columns col [col right-column] (fn [x rx] (- x rx)))
        (tc/drop-columns [right-column]))))

#_(def Mainstream-joiners-projected-to-fall
    {:title "Mainstream joiners projected to fall"
     :chart (let [joiners (-> (get-in joiners-by-setting-summaries [:total-summary :table]) ;; can I add in net change?
                              (tc/select-rows #(= "Mainstream" (:setting %)))
                              (tc/add-column :transition "Joiners"))
                  leavers (-> (get-in leavers-by-setting-summaries [:total-summary :table])
                              (tc/select-rows #(= "Mainstream" (:setting %)))
                              (tc/add-column :transition "Leavers"))
                  net (-> joiners
                          (tc/left-join leavers :calendar-year)
                          (map-net-diff :min)
                          (map-net-diff :p05)
                          (map-net-diff :q1)
                          (map-net-diff :median)
                          (map-net-diff :q3)
                          (map-net-diff :p95)
                          (map-net-diff :max)
                          (tc/drop-columns #":right.*")
                          (tc/add-column :transition "Net change"))
                  data (tc/concat joiners leavers net)]
              (merge (td/total-summary-plot {:data data
                                             :colors-and-shapes (acc/color-and-shape-lookup ["Joiners" "Leavers" "Net change"])
                                             :order-field :transition
                                             :label-field :transition
                                             :group-title "Setting"})
                     {:title "Actual/Projected Count of New EHCPs for Mainstream settings"}))
     :text ["The model has projected an average rate of joiners due to the high variability historically"
            "This rate is then falling in line with the drop in the background population"
            "Leavers are projected to rise slightly, due to a larger population, then remain fairly static"
            "All of this leads to a very gradual population in decline long term"]
     :slide-type ::was/title-two-columns-slide
     :left-box ::was/chart
     :right-box ::was/text})

(def mainstream-settings
  {:title "TBC"
   :chart {:vconcat [(let [data (-> setting-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Mainstream" (:setting-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes setting-colours
                                               :order-field :setting
                                               :label-field :setting
                                               :group-title "Setting"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per Mainstream Settings"
                                               :chart-height 200}))
                     (let [data (-> setting-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Mainstream" (:setting-group %)))
                                    (tc/add-columns {:min 0.0
                                                     :p05 0.0
                                                     :q1 0.0
                                                     :q3 0.0
                                                     :p95 0.0
                                                     :max 0.0
                                                     :observations 1000}))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes setting-colours
                                                  :order-field :setting
                                                  :label-field :setting
                                                  :group-title "Setting"
                                                  :chart-title "Median % EHCP change year on year by Mainstream Settings"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def special-settings
  {:title "TBC"
   :chart {:vconcat [(let [data (-> setting-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Special" (:setting-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes setting-colours
                                               :order-field :setting
                                               :label-field :setting
                                               :group-title "Setting"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per Specialist Setting"
                                               :chart-height 200}))
                     (let [data (-> setting-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Special" (:setting-group %)))
                                    (tc/add-column :min 0.0)
                                    (tc/add-column :p05 0.0)
                                    (tc/add-column :q1 0.0)
                                    (tc/add-column :q3 0.0)
                                    (tc/add-column :p95 0.0)
                                    (tc/add-column :max 0.0)
                                    (tc/add-column :observations 1000))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes setting-colours
                                                  :order-field :setting
                                                  :label-field :setting
                                                  :group-title "Setting"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-title "Median % EHCP change year on year by Specialist Setting"
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def other-settings
  {:title "TBC"
   :chart {:vconcat [(let [data (-> setting-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Other" (:setting-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes setting-colours
                                               :order-field :setting
                                               :label-field :setting
                                               :group-title "Setting"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per \"Other\" Setting"
                                               :chart-height 200}))
                     (let [data (-> setting-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :setting-group [:setting]
                                                    (fn [s] (g/setting-groups s)))
                                    (tc/select-rows #(= "Other" (:setting-group %)))
                                    (tc/add-columns {:min 0.0
                                                     :p05 0.0
                                                     :q1 0.0
                                                     :q3 0.0
                                                     :p95 0.0
                                                     :max 0.0
                                                     :observations 1000}))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes setting-colours
                                                  :order-field :setting
                                                  :label-field :setting
                                                  :group-title "Setting"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-title "Median % EHCP change year on year by \"Other\" Setting"
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def primary-needs
  {:title "TBC"
   :chart (let [data (-> need-summaries
                         (get-in [:total-summary :table]))]
            (td/total-summary-plot {:data data
                                    :colors-and-shapes need-colours
                                    :order-field :need
                                    :label-field :need
                                    :group-title "Primary Need"
                                    :x-axis-font-size 16
                                    :x-axis-title-size 16
                                    :y-axis-title-size 16
                                    :y-axis-font-size 16
                                    :chart-title "Count of EHCPs per Primary Need"}))
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def interaction-needs
  {:title "TBC"
   :chart {:vconcat [(let [data (-> need-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Interaction Needs" (:need-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes need-colours
                                               :order-field :need
                                               :label-field :need
                                               :group-title "Primary Need"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per Need"
                                               :chart-height 200}))
                     (let [data (-> need-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Interaction Needs" (:need-group %)))
                                    (tc/add-columns {:min 0.0
                                                     :p05 0.0
                                                     :q1 0.0
                                                     :q3 0.0
                                                     :p95 0.0
                                                     :max 0.0
                                                     :observations 1000}))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes need-colours
                                                  :order-field :need
                                                  :label-field :need
                                                  :group-title "Primary Need"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-title "Median % EHCP change year on year by Need"
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def learning-needs
  {:title "TBC"
   :chart {:vconcat [(let [data (-> need-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Learning Needs" (:need-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes need-colours
                                               :order-field :need
                                               :label-field :need
                                               :group-title "Primary Need"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per Need"
                                               :chart-height 200}))
                     (let [data (-> need-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Learning Needs" (:need-group %)))
                                    (tc/add-columns {:min 0.0
                                                     :p05 0.0
                                                     :q1 0.0
                                                     :q3 0.0
                                                     :p95 0.0
                                                     :max 0.0
                                                     :observations 1000}))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes need-colours
                                                  :order-field :need
                                                  :label-field :need
                                                  :group-title "Primary Need"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-title "Median % EHCP change year on year by setting"
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def physical-and-sensory-needs
  {:title "TBC"
   :chart {:vconcat [(let [data (-> need-summaries
                                    (get-in [:total-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Physical & Sensory Needs" (:need-group %))))]
                       (td/total-summary-plot {:data data
                                               :colors-and-shapes need-colours
                                               :order-field :need
                                               :label-field :need
                                               :group-title "Primary Need"
                                               :x-axis-font-size 16
                                               :x-axis-title-size 16
                                               :y-axis-title-size 16
                                               :y-axis-font-size 16
                                               :chart-title "Actual/Projected Count of EHCPs per Need"
                                               :chart-height 200}))
                     (let [data (-> need-summaries
                                    (get-in [:pct-diff-summary :table])
                                    (tc/map-columns :need-group [:need]
                                                    (fn [n] (g/need-groups n)))
                                    (tc/select-rows #(= "Physical & Sensory Needs" (:need-group %)))
                                    (tc/add-columns {:min 0.0
                                                     :p05 0.0
                                                     :q1 0.0
                                                     :q3 0.0
                                                     :p95 0.0
                                                     :max 0.0
                                                     :observations 1000}))]
                       (td/pct-diff-summary-plot {:data data
                                                  :colors-and-shapes need-colours
                                                  :order-field :need
                                                  :label-field :need
                                                  :group-title "Primary Need"
                                                  :x-axis-font-size 16
                                                  :x-axis-title-size 16
                                                  :y-axis-title-size 16
                                                  :y-axis-font-size 16
                                                  :chart-title "Median % EHCP change year on year by setting"
                                                  :chart-height 200}))]}
   :text ["TBC"]
   :slide-type ::was/title-two-columns-slide
   :left-box ::was/chart
   :right-box ::was/text})

(def phases
  {:title "TBC"
   :chart (let [data (-> phase-summaries
                         (get-in [:total-summary :table])
                         (tc/drop-rows #(= "Outside of SEND age" (:phase %))))]
            (td/total-summary-plot {:data data
                                    :colors-and-shapes (acc/color-and-shape-lookup (into (sorted-set) (:phase data)))
                                    :order-field :phase
                                    :label-field :phase
                                    :x-axis-font-size 16
                                    :x-axis-title-size 16
                                    :y-axis-title-size 16
                                    :y-axis-font-size 16
                                    :chart-title "Count of EHCPs per Phase"}))
   :text ["TBC"]
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

(
;;; Notebook
 )

{:nextjournal.clerk/visibility {:result :show}}

(sl/slide title-content)

;; ---

(sl/slide agenda-content)

;; ---

(sl/slide summary)

;; ---

(sl/slide baseline-description)

;; ---

(sl/slide modelling)

;; ---

(sl/slide historic-total-ehcp-growth)

;; ---

(sl/slide comparison-of-projection-vs-current-counts)

;; ---

(sl/slide echp-rates)

;; ---

(sl/slide overall-ehcp-projection)

;; ---

(sl/slide settings-projection)

;; ---

(sl/slide mainstream-settings)

;; ---

(sl/slide special-settings)

;; ---

(sl/slide other-settings)

;;---

(sl/slide primary-needs)

;; ---

(sl/slide interaction-needs)

;; ---

(sl/slide learning-needs)

;; ---

(sl/slide physical-and-sensory-needs)

;; ---

(sl/slide phases)

;; ---

(sl/slide conclusions)

;; ---

(sl/slide next-steps)

{:nextjournal.clerk/visibility {:result :hide}}
