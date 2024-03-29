(ns witan.send.adroddiad.census
  (:require [clojure.core.async :as a]
            [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.gradient :as dt-grad]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [witan.send.adroddiad.summary :as summary]))

(defn census-domain [census]
  (let [ays (into (sorted-set) (-> census :academic-year))
        needs (into (sorted-set) (-> census :need))
        settings (into (sorted-set) (-> census :setting))]
    (into []
          cat
          [ays needs settings])))

(def settings-sections [{:title "All Settings" :series (sorted-set "EYS" "FEC" "IMS" "ISS" "ISSR" "MMSIA" "MMSOA"
                                                                   "MSSIA" "MSSOA" "MSSR" "MU" "MUOA" "OTH" "SP16"
                                                                   "NEET")}
                        {:title "Mainstream" :series (sorted-set "EYS" "FEC" "IMS" "MMSIA" "MMSOA" "MU" "MUOA")}
                        {:title "Special" :series (sorted-set "ISS" "ISSR" "MSSIA" "MSSOA" "MSSR" "NMSS" "NMSSR")}
                        {:title "Post 16" :series (sorted-set "FEC" "SP16")}
                        {:title "Other" :series (sorted-set "NEET" "OTH")}])

(def needs-sections [{:title "All Needs" :series (sorted-set "ASD" "HI" "MLD" "MSI" "PD" "PMLD" "SEMH" "SLCN" "SLD" "SPLD" "VI")}
                     {:title "Physical Needs" :series (sorted-set "HI" "MSI" "PD" "VI")}

                     {:title "Interaction Needs" :series (sorted-set "ASD" "SEMH" "SLCN")}
                     {:title "Learning Needs" :series (sorted-set "MLD" "PMLD" "SLD" "SPLD")}])

(def ncy-sections [{:title "All NCYs" :series (sorted-set -3 -2 -1 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21)}
                   {:title "Early years" :series (sorted-set -3 -2 -1 0)}
                   {:title "Key Stage 1" :series (sorted-set 1 2)}
                   {:title "Key Stage 2" :series (sorted-set 3 4 5 6)}
                   {:title "Key Stage 3" :series (sorted-set 7 8 9)}
                   {:title "Key Stage 4" :series (sorted-set 10 11)}
                   {:title "Key Stage 5" :series (sorted-set 12 13 14)}
                   {:title "NCY 15+" :series (sorted-set 15 16 17 18 19 20 21)}])

(defn census-report-data
  ([{:keys [census-data series-key value-key]}]
   (let [base-report (-> census-data
                         (tc/group-by [:simulation :calendar-year series-key])
                         (tc/aggregate {value-key #(dfn/sum (value-key %))})
                         (summary/seven-number-summary [:calendar-year series-key] value-key))
         totals (-> census-data
                    (tc/group-by [:simulation :calendar-year])
                    (tc/aggregate {value-key #(dfn/sum (value-key %))})
                    (summary/seven-number-summary [:calendar-year] value-key)
                    (tc/order-by [:calendar-year])
                    (tc/select-columns [:calendar-year :median])
                    (tc/rename-columns {:median :total-median}))
         yoy-diffs (-> base-report
                       (tc/order-by [series-key :calendar-year])
                       (tc/group-by [series-key])
                       (tc/add-columns {:median-yoy-diff (fn add-year-on-year [ds]
                                                           (let [medians (:median ds)
                                                                 diffs (dt-grad/diff1d medians)]
                                                             (into [] cat [[0] diffs])))
                                        :median-yoy-%-diff (fn add-year-on-year-% [ds]
                                                             (let [medians (:median ds)
                                                                   diffs (dt-grad/diff1d medians)
                                                                   diff-%s (let [medians-but-last (map #(if (zero? %) 1 %) (drop-last (:median ds)))]
                                                                             (dfn// diffs medians-but-last))]
                                                               (into [] cat [[0] diff-%s])))})
                       (tc/ungroup)
                       (tc/select-columns [:calendar-year series-key :median-yoy-diff :median-yoy-%-diff]))]
     (-> base-report
         (tc/inner-join totals [:calendar-year])
         (tc/map-columns :pct-of-total [:median :total-median] (fn [m tm] (if (or (zero? m)
                                                                                 (zero? tm))
                                                                           nil
                                                                           (float (/ m tm)))))
         (tc/inner-join yoy-diffs [:calendar-year series-key])
         (tc/select-columns [:calendar-year series-key :min :low-95 :q1 :median :q3 :high-95 :max :total-median :pct-of-total :median-yoy-diff :median-yoy-%-diff])
         (tc/order-by [:calendar-year series-key]))))
  ([census-data options]
   (census-report-data (assoc options :census-data census-data))))

(defn census-report [{:keys [census-data colors-and-shapes series-key legend-label report-sections
                             file-name watermark data-table-formatf base-chart-spec value-key
                             bottom top ;; define y-axis
                             ]
                      :or {data-table-formatf identity
                           watermark ""
                           base-chart-spec plot/base-pop-chart-spec
                           value-key :transition-count
                           report-sections (cond
                                             (= series-key :setting)
                                             settings-sections

                                             (= series-key :need)
                                             needs-sections

                                             (= series-key :academic-year)
                                             ncy-sections)}}]
  (println (str "Building " file-name))
  (let [data (census-report-data {:census-data census-data
                                  :series-key series-key
                                  :value-key value-key})
        plot-index-f (fn [m]
                       (if (and bottom top)
                         (plot/stated-y-index (merge m {::plot/bottom bottom ::plot/top top}))
                         (plot/zero-y-index m)))]
    (try (-> (into []
                   (keep (fn [{:keys [title sheet-name series]
                               :or {sheet-name title}
                               :as conf}]
                           (try (let [series-selector-f (into #{} series)
                                      data-table (-> data
                                                     (tc/select-rows #(boolean (series-selector-f (series-key %))))
                                                     (tc/order-by [series-key :calendar-year]))
                                      grouped-data (-> data-table
                                                       (tc/group-by [series-key]))]
                                  (when (< 0 (tc/row-count data-table))
                                    (-> (series/grouped-ds->median-iqr-95-series-and-legend {::series/colors-and-shapes colors-and-shapes
                                                                                             ::series/grouped-data grouped-data
                                                                                             ::series/series-key series-key})
                                        (merge {::plot/legend-label legend-label
                                                ::plot/title {::plot/label title}}
                                               base-chart-spec
                                               {::large/data (data-table-formatf data-table)
                                                ::large/sheet-name sheet-name})
                                        (plot/add-overview-legend-items)
                                        plot-index-f
                                        (update ::plot/canvas plot/add-watermark watermark)
                                        (chart-utils/->large-charts))))
                                (catch Exception e (throw (ex-info (format "Failed to create %s" title) conf e))))))
                   report-sections)
             (large/create-workbook)
             (large/save-workbook! file-name))
         (println (str file-name " complete!"))
         {:file-name file-name}
         (catch Exception e (println "Failed to create " file-name)
                (throw (ex-info (str "Failed to create " file-name) {:file-name file-name} e))))))

(defn census-reports [chart-defs]
  (let [concurrent (max 2 (dec (.. Runtime getRuntime availableProcessors)))
        output-chan (a/chan)]
    (a/pipeline-blocking concurrent
                         output-chan
                         (map census-report)
                         (a/to-chan! chart-defs))
    (a/<!! (a/into [] output-chan))))

(defn census-analysis [{:keys [simulated-transitions colors-and-shapes start-year base-out-dir watermark
                               settings-sections needs-sections ncy-sections]
                        :or {settings-sections settings-sections
                             needs-sections needs-sections
                             ncy-sections ncy-sections}}]
  (let [census-data        (stc/transition-counts->census-counts simulated-transitions start-year)
        joiners            (-> simulated-transitions
                               (tc/select-rows #(= "NONSEND" (get % :setting-1)))
                               (tc/map-columns :calendar-year-2 [:calendar-year] inc)
                               (tc/select-columns [:calendar-year-2 :transition-count :simulation :academic-year-2 :need-2 :setting-2])
                               (tc/rename-columns {:calendar-year-2 :calendar-year
                                                   :academic-year-2 :academic-year
                                                   :setting-2       :setting
                                                   :need-2          :need}))
        movers-in          (-> simulated-transitions
                               (tc/select-rows #(not= (:setting-1 %) (:setting-2 %)))
                               (tc/map-columns :calendar-year-2 [:calendar-year] inc)
                               (tc/select-columns [:calendar-year-2 :academic-year-2 :setting-2 :need-2 :transition-count :simulation])
                               (tc/rename-columns {:calendar-year-2 :calendar-year
                                                   :academic-year-2 :academic-year
                                                   :setting-2       :setting
                                                   :need-2          :need}))
        joiners-movers-in  (tc/concat-copying joiners movers-in)
        leavers            (-> simulated-transitions
                               (tc/select-rows #(= "NONSEND" (get % :setting-2)))
                               (tc/select-columns [:calendar-year :transition-count :simulation :academic-year-1 :need-1 :setting-1])
                               (tc/rename-columns {:academic-year-1 :academic-year
                                                   :setting-1       :setting
                                                   :need-1          :need}))
        movers-out         (-> simulated-transitions
                               (tc/select-rows #(not= (:setting-1 %) (:setting-2 %)))
                               (tc/select-columns [:calendar-year :academic-year-1 :setting-1 :need-1 :transition-count :simulation])
                               (tc/rename-columns {:academic-year-1 :academic-year
                                                   :setting-1       :setting
                                                   :need-1          :need}))
        leavers-movers-out (tc/concat-copying leavers movers-out)]
    {:census-data census-data
     :joiners joiners
     :movers-in movers-in
     :joiners-movers-in joiners-movers-in
     :leavers leavers
     :movers-out movers-out
     :leavers-movers-out leavers-movers-out
     :settings-sections settings-sections
     :needs-sections needs-sections
     :ncy-sections ncy-sections}))
