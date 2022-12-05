(ns witan.send.adroddiad.ncy-analysis
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

(defn delta-column [ds column-name]
  (tc/map-columns ds column-name [(name column-name) (str "[:calendar-year]-aggregation." (name column-name))] (fn [a b] (- a b))))

(defn ncy-analysis [simulated-transition-counts ncy]
  (let [min-year (stc/min-calendar-year simulated-transition-counts)
        max-year (dfn/reduce-max (:calendar-year simulated-transition-counts))
        year-range (into (sorted-set) (range min-year (+ max-year 1)))
        ncy-analysis (try {:ncy-total (-> simulated-transition-counts
                                          (tr/transitions->census min-year)
                                          (tc/select-rows #(= ncy (:academic-year %)))
                                          (tc/group-by [:simulation :calendar-year])
                                          (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                                          (summary/seven-number-summary [:calendar-year] :transition-count)
                                          (tc/order-by [:calendar-year]))
                           :ncy-joiners (let [initial-ds (-> simulated-transition-counts
                                                             (tc/select-rows #(= ncy (:academic-year-2 %)))
                                                             (tr/joiners-to)
                                                             (tr/transitions->census min-year)
                                                             (tc/group-by [:simulation :calendar-year])
                                                             (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                                                             (summary/seven-number-summary [:calendar-year] :transition-count))
                                              years (into (sorted-set) (:calendar-year initial-ds))]
                                          (-> (reduce #(tc/concat-copying %1 (tc/dataset {:low-95 0 :min 0 :q1 0 :q3 0 :median 0 :max 0 :row-count 1 :high-95 0 :calendar-year %2}))
                                                      initial-ds (clojure.set/difference year-range years))
                                              (tc/order-by [:calendar-year])))
                           :ncy-leavers (let [initial-ds (-> simulated-transition-counts
                                                             (tc/select-rows #(= ncy (:academic-year-1 %)))
                                                             (tr/leavers-from)
                                                             (tc/group-by [:simulation :calendar-year])
                                                             (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                                                             (summary/seven-number-summary [:calendar-year] :transition-count))
                                              years (into (sorted-set) (:calendar-year initial-ds))]
                                          (-> (reduce #(tc/concat-copying %1 (tc/dataset {:low-95 0 :min 0 :q1 0 :q3 0 :median 0 :max 0 :row-count 1 :high-95 0 :calendar-year %2}))
                                                      initial-ds (clojure.set/difference year-range years))
                                              (tc/order-by [:calendar-year])))}
                          (catch Exception ne
                            (throw
                             (ex-info
                              (ex-message ne)
                              {:message "Something wrong wih NCY provided"
                               :causes #{:ncy ncy}}
                              ne))))]
    (assoc ncy-analysis :ncy-net
           (-> (tc/left-join (tc/rename-columns (:ncy-joiners ncy-analysis) (comp name))
                             (tc/rename-columns (:ncy-leavers ncy-analysis) (comp name)) "calendar-year")
               (delta-column :low-95)
               (delta-column :min)
               (delta-column :q1)
               (delta-column :q3)
               (delta-column :median)
               (delta-column :max)
               (delta-column :high-95)
               (tc/rename-columns {"row-count" :row-count "calendar-year" :calendar-year})
               (tc/select-columns [:low-95 :min :q1 :q3 :median :max :row-count :high-95 :calendar-year])))))

(defn ncy-analysis-summary [ncy-analysis-map]
  (-> (apply tc/concat-copying
             (into []
                   (map (fn [[k ds]] (-> ds (tc/add-column :transition-type k))))
                   ncy-analysis-map))
      (tc/select-columns [:calendar-year :transition-type :median])
      (tc/pivot->wider [:transition-type] [:median])
      (tc/map-columns :next-year-total [:ncy-total :ncy-joiners :ncy-leavers]
                      (fn [total joiners leavers] (- (+ total joiners)
                                                     (+ leavers))))))

(defn ncy-analysis-chart [ncy-analysis-map
                          ncy
                          {:keys [x-axis y-axis size legend-font legend-font-size title title-format watermark] :as _chart-config
                           :or {x-axis {::tick-formatter int ::label "Calendar Year"}
                                y-axis {::tick-formatter int ::label "Number of CYP"}
                                size {:width 1539
                                      :height 1037
                                      :background colors/white}
                                legend-font "Open Sans Bold"
                                legend-font-size 24
                                title (str ncy " analysis")
                                title-format {:font-size 36 :font "Open Sans Bold" :font-style :bold :margin 36}
                                watermark (str ncy " analysis")}}]
  (let [{:keys [ncy-total ncy-joiners ncy-movers-in ncy-leavers ncy-movers-out ncy-net] :as _ncy-analysis} ncy-analysis-map
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
                                      (update 2 assoc :label (str "Net change for " ncy)))))
                     (series/ds->median-iqr-95-series
                      ncy-net
                      colors/mc-light-green \v))
               (into [[:grid nil {:position [0 2]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 2])
                                      (update 2 assoc :label (str "Joiners to " ncy)))))
                     (series/ds->median-iqr-95-series
                      (-> ncy-joiners
                          (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (+ cy 0.5))))
                      colors/mc-dark-blue \V))
               (into [[:grid nil {:position [0 1]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 1])
                                      (update 2 assoc :label (str "Total Population for " ncy)))))
                     (series/ds->median-iqr-95-series
                      ncy-total colors/mc-dark-green \O))
               (into [[:grid nil {:position [0 0]}]]
                     (map (fn [s] (-> s
                                      (update 2 assoc :position [0 0])
                                      (update 2 assoc :label (str "Leavers from " ncy)))))
                     (series/ds->median-iqr-95-series
                      (-> ncy-leavers
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
                                                  [:shape (str "Joiners to " ncy)
                                                   {:color  colors/mc-dark-blue
                                                    :shape  \A
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Number of CYP in " ncy)
                                                   {:color  colors/mc-dark-green
                                                    :shape  \O
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Leavers from " ncy)
                                                   {:color  colors/mc-orange
                                                    :shape  \V
                                                    :size   15
                                                    :stroke {:size 4.0}}]
                                                  [:shape (str "Net change for " ncy)
                                                   {:color  colors/mc-light-green
                                                    :shape  \v
                                                    :size   15
                                                    :stroke {:size 4.0}}]])
        (plotr/render-lattice size)
        (plot/add-watermark watermark))))


(defn ncy-analysis-report
  ([simulated-transitions file-name]
   (let [ncys (into (sorted-set)
                    (-> simulated-transitions
                        (tc/select-rows #(= -1 (:simulation %)))
                        (tc/drop-rows #(= "NONSEND" (:setting-1 %)))
                        :academic-year-1))]
     (ncy-analysis-report simulated-transitions file-name ncys)))
  ([simulated-transitions file-name ncys]
   (-> (into []
             (comp
              (map (fn [ncy]
                     (let [sam (ncy-analysis simulated-transitions ncy)]
                       (-> {::large/sheet-name (str "NCY " ncy)
                            ::plot/canvas      (ncy-analysis-chart sam ncy {})
                            ::large/data       (ncy-analysis-summary sam)}
                           (chart-utils/->large-charts))))))
             ncys)
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
