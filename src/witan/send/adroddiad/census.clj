(ns witan.send.adroddiad.census
  (:require [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [clojure.core.async :as a]))

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

(defn census-report [{:keys [census-data colors-and-shapes series-key legend-label report-sections
                             file-name watermark base-chart-spec value-key]
                      :or {watermark ""
                           base-chart-spec plot/base-pop-chart-spec
                           value-key :transition-count}}]
  (println (str "Building " file-name))
  (let [data (-> census-data
                 (tc/group-by [:simulation :calendar-year series-key])
                 (tc/aggregate {value-key #(dfn/sum (value-key %))})
                 (summary/seven-number-summary [:calendar-year series-key] value-key)
                 (tc/order-by [:calendar-year series-key]))]
    (try (-> (into []
                   (map (fn [{:keys [title sheet-name series]
                              :or {sheet-name title}}]
                          (let [data-table (-> data
                                               (tc/select-rows #(series (series-key %)))
                                               (tc/order-by [series-key :calendar-year]))
                                grouped-data (-> data-table
                                                 (tc/group-by [series-key]))]
                            (-> (series/grouped-ds->median-iqr-95-series-and-legend {::series/colors-and-shapes colors-and-shapes
                                                                                     ::series/grouped-data grouped-data
                                                                                     ::series/series-key series-key})
                                (merge {::plot/legend-label legend-label
                                        ::plot/title {::plot/label title}}
                                       base-chart-spec
                                       {::large/data data-table
                                        ::large/sheet-name sheet-name})
                                (plot/add-overview-legend-items)
                                (plot/zero-y-index)
                                (update ::plot/canvas plot/add-watermark watermark)
                                (chart-utils/->large-charts)))))
                   report-sections)
             (large/create-workbook)
             (large/save-workbook! file-name))
         (println (str file-name " complete!"))
         {:file-name file-name}
         (catch Exception e (println "Failed to create " file-name)
                (ex-info (str "Failed to create " file-name) {:file-name file-name} e)))))

(defn census-reports [chart-defs]
  (let [concurrent (max 2 (dec (.. Runtime getRuntime availableProcessors)))
        output-chan (a/chan)]
    (a/pipeline-blocking concurrent
                         output-chan
                         (map census-report)
                         (a/to-chan chart-defs))
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
