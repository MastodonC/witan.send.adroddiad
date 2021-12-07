(ns witan.send.adroddiad.need-analysis
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

(defn need-analysis [simulated-transition-counts need]
  (let [min-year (stc/min-calendar-year simulated-transition-counts)]
    {:need-total (-> simulated-transition-counts
                     (tr/transitions->census min-year)
                     (tc/select-rows #(= need (:need %)))
                     (tc/group-by [:simulation :calendar-year])
                     (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                     (summary/seven-number-summary [:calendar-year] :transition-count)
                     (tc/order-by [:calendar-year]))
     :need-joiners (-> simulated-transition-counts
                       (tc/select-rows #(= need (:need-2 %)))
                       (tr/joiners-to)
                       (tr/transitions->census min-year)
                       (tc/group-by [:simulation :calendar-year])
                       (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                       (summary/seven-number-summary [:calendar-year] :transition-count)
                       (tc/order-by [:calendar-year]))
     :need-leavers (-> simulated-transition-counts
                       (tc/select-rows #(= need (:need-1 %)))
                       (tr/leavers-from)
                       (tc/group-by [:simulation :calendar-year])
                       (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                       (summary/seven-number-summary [:calendar-year] :transition-count)
                       (tc/order-by [:calendar-year]))}))

(defn need-analysis-summary [need-analysis-map]
  (-> (apply tc/concat-copying
             (into []
                   (map (fn [[k ds]] (-> ds (tc/add-column :transition-type k))))
                   need-analysis-map))
      (tc/select-columns [:calendar-year :transition-type :median])
      (tc/pivot->wider [:transition-type] [:median])
      (tc/map-columns :next-year-total ["need-total" "need-joiners" "need-leavers"]
                      (fn [total joiners leavers] (- (+ total joiners)
                                                     (+ leavers))))))

(defn need-analysis-chart [need-analysis-map
                           need
                           {:keys [x-axis y-axis size legend-font legend-font-size title title-format watermark] :as _chart-config
                            :or {x-axis {::tick-formatter int ::label "Calendar Year"}
                                 y-axis {::tick-formatter int ::label "Number of CYP"}
                                 size {:width 1539
                                       :height 1037
                                       :background colors/white}
                                 legend-font "Open Sans Bold"
                                 legend-font-size 24
                                 title (str need " analysis")
                                 title-format {:font-size 36 :font "Open Sans Bold" :font-style :bold :margin 36}
                                 watermark (str need " analysis")}}]
  (let [{:keys [need-total need-joiners need-movers-in need-leavers need-movers-out] :as _need-analysis} need-analysis-map
        _cfg (swap! cfg/configuration
                    (fn [c]
                      (-> c
                          (assoc-in [:legend :font] legend-font)
                          (assoc-in [:legend :font-size] legend-font-size))))]
    (-> (into []
              cat
              [(into [[:grid nil {:position [0 3]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 3])
                                      (update 2 assoc :label (str "Joiners to " need)))))
                     (series/ds->median-iqr-95-series
                      (-> need-joiners
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-dark-blue \V))
               (into [[:grid nil {:position [0 2]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 2])
                                      (update 2 assoc :label (str "Total Population for " need)))))
                     (series/ds->median-iqr-95-series
                      need-total colors/mc-dark-green \O))
               (into [[:grid nil {:position [0 1]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 1])
                                      (update 2 assoc :label (str "Leavers from " need)))))
                     (series/ds->median-iqr-95-series
                      (-> need-leavers
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-orange \A))])
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
                                                  [:shape (str "Movers to " need)
                                                   {:color  colors/mc-dark-blue
                                                    :shape  \^
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Joiners to " need)
                                                   {:color  colors/mc-dark-blue
                                                    :shape  \A
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Number of CYP in " need)
                                                   {:color  colors/mc-dark-green
                                                    :shape  \O
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Leavers from " need)
                                                   {:color  colors/mc-orange
                                                    :shape  \V
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Movers from " need)
                                                   {:color  colors/mc-orange
                                                    :shape  \v
                                                    :size   15
                                                    :stroke {:size 4.0}}]])
        (plotr/render-lattice size)
        (plot/add-watermark watermark))))


(defn need-analysis-report [simulated-transitions file-name]
  (let [needs (into (sorted-set)
                    (-> simulated-transitions
                        (tc/select-rows #(= -1 (:simulation %)))
                        (tc/drop-rows #(= "NONSEND" (:need-1 %)))
                        :need-1))]
    (-> (into []
              (comp
               (map (fn [need]
                      (let [sam (need-analysis simulated-transitions need)]
                        (-> {::large/sheet-name need
                             ::plot/canvas      (need-analysis-chart sam need {})
                             ::large/data       (need-analysis-summary sam)}
                            (chart-utils/->large-charts))))))
              needs)
        (large/create-workbook)
        (large/save-workbook! file-name))))

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
