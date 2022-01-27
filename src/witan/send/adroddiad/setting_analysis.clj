(ns witan.send.adroddiad.setting-analysis
  (:require [cljplot.build :as plotb]
            [cljplot.config :as cfg]
            [cljplot.render :as plotr]
            [kixi.large :as large]
            [kixi.plot :as plot]
            [kixi.plot.colors :as colors]
            [kixi.plot.series :as series]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.chart-utils :as chart-utils]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.transitions :as tr]))

(defn setting-analysis [simulated-transition-counts setting]
  (let [min-year (stc/min-calendar-year simulated-transition-counts)]
    {:setting-total (-> simulated-transition-counts
                        (tr/transitions->census min-year)
                        (tc/select-rows #(= setting (:setting %)))
                        (tc/group-by [:simulation :calendar-year])
                        (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                        (summary/seven-number-summary [:calendar-year] :transition-count)
                        (tc/order-by [:calendar-year]))
     :setting-joiners (-> simulated-transition-counts
                          (tr/joiners-to setting)
                          (tr/transitions->census min-year)
                          (tc/group-by [:simulation :calendar-year])
                          (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                          (summary/seven-number-summary [:calendar-year] :transition-count)
                          (tc/order-by [:calendar-year]))
     :setting-movers-in (-> simulated-transition-counts
                            (tr/movers-to setting)
                            (tc/group-by [:simulation :calendar-year])
                            (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                            (summary/seven-number-summary [:calendar-year] :transition-count)
                            (tc/order-by [:calendar-year]))
     :setting-leavers (-> simulated-transition-counts
                          (tr/leavers-from setting)
                          (tc/group-by [:simulation :calendar-year])
                          (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                          (summary/seven-number-summary [:calendar-year] :transition-count)
                          (tc/order-by [:calendar-year]))
     :setting-movers-out (-> simulated-transition-counts
                             (tr/movers-from setting)
                             (tc/group-by [:simulation :calendar-year])
                             (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                             (summary/seven-number-summary [:calendar-year] :transition-count)
                             (tc/order-by [:calendar-year]))}))

(defn setting-analysis-summary [setting-analysis-map]
  (-> (apply tc/concat-copying
             (into []
                   (map (fn [[k ds]] (-> ds (tc/add-column :transition-type k))))
                   setting-analysis-map))
      (tc/select-columns [:calendar-year :transition-type :median])
      (tc/pivot->wider [:transition-type] [:median])
      (tc/map-columns :next-year-total ["setting-total" "setting-joiners" "setting-movers-in" "setting-leavers" "setting-movers-out"]
                      (fn [total joiners in leavers out] (- (+ total joiners in)
                                                            (+ leavers out))))))

(defn setting-analysis-chart [setting-analysis-map
                              setting
                              {:keys [x-axis y-axis size legend-font legend-font-size title title-format watermark] :as _chart-config
                               :or {x-axis {::tick-formatter int ::label "Calendar Year"}
                                    y-axis {::tick-formatter int ::label "Number of CYP"}
                                    size {:width 1539
                                          :height 1037
                                          :background colors/white}
                                    legend-font "Open Sans Bold"
                                    legend-font-size 24
                                    title (str setting " analysis")
                                    title-format {:font-size 36 :font "Open Sans Bold" :font-style :bold :margin 36}
                                    watermark (str setting " analysis")}}]
  (let [{:keys [setting-total setting-joiners setting-movers-in setting-leavers setting-movers-out] :as _setting-analysis} setting-analysis-map
        _cfg (swap! cfg/configuration
                    (fn [c]
                      (-> c
                          (assoc-in [:legend :font] legend-font)
                          (assoc-in [:legend :font-size] legend-font-size))))]
    (-> (into []
              cat
              [(into [[:grid nil {:position [0 4]}]]
                     (map (fn [s] (-> s
                                     (update 2 assoc :position [0 4])
                                     (update 2 assoc :label (str "Movers to " setting)))))
                     (series/ds->median-iqr-95-series
                      (-> setting-movers-in
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-dark-blue \v))
               (into [[:grid nil {:position [0 3]}]]
                     (map (fn [s] (-> s
                                     (update 2 assoc :position [0 3])
                                     (update 2 assoc :label (str "Joiners to " setting)))))
                     (series/ds->median-iqr-95-series
                      (-> setting-joiners
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-dark-blue \V))
               (into [[:grid nil {:position [0 2]}]]
                     (map (fn [s] (-> s
                                     (update 2 assoc :position [0 2])
                                     (update 2 assoc :label (str "Total Population for " setting)))))
                     (series/ds->median-iqr-95-series
                      setting-total colors/mc-dark-green \O))
               (into [[:grid nil {:position [0 1]}]]
                     (map (fn [s] (-> s
                                     (update 2 assoc :position [0 1])
                                     (update 2 assoc :label (str "Leavers from " setting)))))
                     (series/ds->median-iqr-95-series
                      (-> setting-leavers
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-orange \A))
               (into [[:grid nil {:position [0 0]}]]
                     (map (fn [s] (-> s
                                     (update 2 assoc :position [0 0])
                                     (update 2 assoc :label (str "Movers from " setting)))))
                     (series/ds->median-iqr-95-series
                      (-> setting-movers-out
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-orange \^))])
        (plotb/preprocess-series)
        (plotb/update-scales :x :fmt (::tick-formatter x-axis))
        (plotb/update-scales :y :fmt (::tick-formatter y-axis))
        (plotb/add-axes :bottom {:ticks {:font-size 12 :font-style nil}})
        (plotb/add-axes :left {:ticks {:font-size 12 :font-style nil}})
        (plotb/add-label :bottom (::label x-axis) {:font-size 36 :font "Open Sans" :font-style nil})
        (plotb/add-label :left (::label y-axis) {:font-size 36 :font "Open Sans" :font-style nil})
        (plotb/add-label :top title title-format)
        (plotb/add-legend "Transition Movements" [[:line "Median/Actual"
                                                   {:color :black :stroke {:size 4} :font "Open Sans"}]
                                                  [:rect "Interquartile range"
                                                   {:color (colors/color :black 50)}]
                                                  [:rect "90% range"
                                                   {:color (colors/color :black 25)}]
                                                  [:shape (str "Movers to " setting)
                                                   {:color  colors/mc-dark-blue
                                                    :shape  \^
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Joiners to " setting)
                                                   {:color  colors/mc-dark-blue
                                                    :shape  \A
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Number of CYP in " setting)
                                                   {:color  colors/mc-dark-green
                                                    :shape  \O
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Leavers from " setting)
                                                   {:color  colors/mc-orange
                                                    :shape  \V
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Movers from " setting)
                                                   {:color  colors/mc-orange
                                                    :shape  \v
                                                    :size   15
                                                    :stroke {:size 4.0}}]])
        (plotr/render-lattice size)
        (plot/add-watermark watermark))))


(defn setting-analysis-report
  ([simulated-transitions file-name options]
   (let [settings (into (sorted-set)
                        (-> simulated-transitions
                            (tc/select-rows #(= -1 (:simulation %)))
                            (tc/drop-rows #(= "NONSEND" (:setting-1 %)))
                            :setting-1))]
     (-> (into []
               (comp
                (map (fn [setting]
                       (let [sam (setting-analysis simulated-transitions setting)]
                         (-> {::large/sheet-name setting
                              ::plot/canvas      (setting-analysis-chart sam setting options)
                              ::large/data       (setting-analysis-summary sam)}
                             (chart-utils/->large-charts))))))
               settings)
         (large/create-workbook)
         (large/save-workbook! file-name))))
  ([simulated-transitions file-name]
   (setting-analysis-report simulated-transitions file-name {})))

(comment

  (require '[tech.v3.datatype.gradient :as dt-grad])
  ;; add in growth numbers
  (tc/add-columns {:median-yoy-diff (fn add-year-on-year [ds]
                                      (let [medians (:median-2021 ds)
                                            diffs (dt-grad/diff1d medians)]
                                        (into [] cat [[0] diffs])))
                   :median-yoy-%-diff (fn add-year-on-year-% [ds]
                                        (let [medians (:median-2021 ds)
                                              diffs (dt-grad/diff1d medians)
                                              diff-%s (dfn// diffs (drop-last (:median-2021 ds)))]
                                          (into [] cat [[0] diff-%s])))})

  )
