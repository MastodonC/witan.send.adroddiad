(ns witan.send.adroddiad.summary.report
  (:require
   [clojure.java.io :as io]
   [com.climate.claypoole.lazy :as lazy]
   [kixi.large :as xl]
   [kixi.plot :as plot]
   [kixi.plot.series :as series]
   [tablecloth.api :as tc]
   [tech.v3.libs.parquet :as parquet]
   [witan.send.adroddiad.summary :as summary]))

(defn friendly-column-names [ds]
  (tc/rename-columns ds
                     {:setting "Setting"
                      :need "Need"
                      :academic-year "NCY"
                      :calendar-year "Calendar Year"
                      :min "min"
                      :low-95 "Low 95%"
                      :q1 "q1"
                      :median "Median/Actual"
                      :q3 "q3"
                      :high-95 "High 95%"
                      :max "Max"}))

(defn format-table [ds domain-key]
  (-> ds
      (tc/reorder-columns [domain-key :calendar-year
                           :min :low-95 :q1
                           :median
                           :q3 :high-95 :max])
      friendly-column-names))

(defn format-presentation-table [ds domain-key]
  (let [years (->> (apply max (-> ds :calendar-year))
                   (iterate #(- % 5))
                   (take 3)
                   reverse
                   set)]
    (-> ds
        (tc/select-rows #(years (:calendar-year %))) ;; we only want the last year, last year -5 and the jump off year
        (tc/select-columns [domain-key :calendar-year
                            :median])
        friendly-column-names)))

(defn sorted-file-list [directory]
  (-> directory
      io/file
      .listFiles
      sort))

(defn simulated-transitions-files [prefix directory]
  (into []
        (comp
         (filter #(re-find (re-pattern (format "%s-[0-9]+\\.parquet$" prefix)) (.getName %)))
         (map #(.getPath %)))
        (sorted-file-list directory)))

(defn read-and-split [ds-split-key file]
  (-> file
      (parquet/parquet->ds {:key-fn keyword})
      (tc/group-by [ds-split-key] {:result-type :as-seq})))

(defn historical-transitions->simulated-counts
  "Aggregate historical transitions from `transition-file` into dataset
  of the form used for simulated transitions, with `:transition-counts`"
  [transition-file]
  (-> transition-file
      (tc/dataset {:key-fn keyword})
      (tc/group-by [:calendar-year
                    :academic-year-1 :need-1 :setting-1
                    :academic-year-2 :need-2 :setting-2])
      (tc/aggregate {:transition-count tc/row-count})
      (tc/map-columns :calendar-year-2 [:calendar-year] inc)
      (tc/add-column :simulation -1)
      (tc/convert-types {:academic-year-1 :int8
                         :academic-year-2 :int8
                         :calendar-year :int16
                         :calendar-year-2 :int16
                         :simulation :int8
                         :transition-count :int64})))

(defn summarise-simulations
  [{:keys [cpu-pool ;; Allows us to share the resources between all the jobs
           historical-transition-file ;; (historical-transitions->simulated-counts (str out-dir "wp-3-5-transitions.csv"))
           simulated-transitions-files ;; (simulated-transitions-files "baseline-results" out-dir)
           ds-split-key                ;; :simulation
           domain-key                  ;; :setting
           order-key ;; :calendar-year ;; this is just the x-key
           value-key ;; :transition-count
           summariser
           simulation-transform ;; (fn [ds]
           ;;   (-> ds
           ;;       (setting-fix)
           ;;       at/transitions->census
           ;;       (tc/group-by [domain-key order-key])
           ;;       (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
           ;;       (tc/order-by [order-key])))
           ]
    :as m
    :or {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (assoc m
         :summary
         (let [historical-transitions (historical-transitions->simulated-counts historical-transition-file)]
           (as-> simulated-transitions-files $
             (lazy/upmap cpu-pool (partial read-and-split ds-split-key) $)
             (mapcat identity $)
             (conj $ historical-transitions)
             (lazy/upmap cpu-pool simulation-transform $)
             (summary/summary-statistics $
                                         [domain-key order-key]
                                         (or summariser
                                             (summary/default-summariser value-key)))
             (tc/order-by $ [order-key])
             (tc/rename-columns $ {domain-key :domain-key})
             (tc/reorder-columns $ [:domain-key order-key])
             (tc/group-by $ [:domain-key] {:result-type :as-map})
             ;; create the data key
             (update-vals $ (fn [ds] {:data ds}))
             (update-keys $ (fn [k]
                              {:domain-value (val (first k))}))))))

(comment
  {{:domain-key :setting :domain-value "NMI"}
   {:data "some tmd"
    :series "a vector of series to be plotted"
    :legend "the legend items that match this series"
    }

   {:domain-key :setting :domain-value "Mainstream"}
   {:data "some other tmd"
    :series "a vector of series to be plotted"
    :legend "the legend items that match this series"}

   }

  [{ ;; lots chart spec stuff
    :data "all the concatenated data shown on this chart"
    :series "all the series for this chart"
    :legends "all the legend items for this chart"
    :chart "the actual chart image"
    }]

  {{:domain-key :setting :domain-value "NMI"}
   {:data "some tmd"
    :series "a vector of series to be plotted"
    :legend "the legend items that match this series"
    }

   {:domain-key :setting :domain-value "Mainstream"}
   {:data "some other tmd"
    :series "a vector of series to be plotted"
    :legend "the legend items that match this series"}

   }

  [{ ;; lots chart spec stuff
    :data "all the concatenated data shown on this chart"
    :series "all the series for this chart"
    :legends "all the legend items for this chart"
    :chart "the actual chart image"
    }])

(defn summary-series [{:keys [summary
                              color-and-shape-map ;; colors-and-shapes
                              order-key]
                       :as m}]
  (assoc m :summary
         (reduce-kv (fn [m k v] ;; FIXME: factor this out into a function that could be passed in, perhaps at the level below the assoc onto the accumulator
                      (try
                        (let [{:keys [domain-value]} k
                              {:keys [data]} v
                              color (-> domain-value color-and-shape-map :color)
                              shape (-> domain-value color-and-shape-map :shape)
                              line-y :median
                              ribbon-1-high-y :q3 ribbon-1-low-y :q1
                              ribbon-2-high-y :high-95 ribbon-2-low-y :low-95]
                          (assoc m k (assoc v
                                            :series
                                            (series/ds->line-and-double-ribbon
                                             data
                                             {:color color :shape shape
                                              :x order-key :line-y line-y
                                              :ribbon-1-high-y ribbon-1-high-y :ribbon-1-low-y ribbon-1-low-y
                                              :ribbon-2-high-y ribbon-2-high-y :ribbon-2-low-y ribbon-2-low-y}))))
                        (catch Exception e
                          (throw (ex-info (format "Failed to create series for %s" (:domain-value k)) {:k k :v v} e)))))
                    {}
                    summary)))

(defn summary-legend [{:keys [summary
                              color-and-shape-map ;; colors-and-shapes
                              ]
                       :as m}]
  (assoc m :summary
         (reduce-kv (fn [m k v]
                      (let [{:keys [domain-value]} k
                            color (-> domain-value color-and-shape-map :color)
                            shape (-> domain-value color-and-shape-map :legend-shape)]
                        (assoc m k (assoc v
                                          :legend
                                          (series/legend-spec domain-value color shape)))))
                    {}
                    summary)))

;; This gets mapped over the chart selectors
(defn summary-chart [{:keys [summary ;; this has all the data and series
                             domain-key]
                      :as summary-items}
                     {:keys [chart-title chart-selector ;; these two are based on what is currently in main-report but done up with keywords and then merged with the rest
                             ;; the above lets me do stuff like have different bottom/top for different charts for comparison
                             ;; bottom top ; These should be partialed in with the chartf
                             sheet-name
                             chartf
                             watermark
                             legend-label
                             base-chart-spec]
                      :as chart-configuration}]
  (let [summary-subset (into (sorted-map-by
                              (fn [key1 key2]
                                (compare (:domain-value key1)
                                         (:domain-value key2))))
                             (select-keys
                              summary
                              (into []
                                    (map (fn [s]
                                           {:domain-value s}))
                                    chart-selector)))
        series (into []
                     (comp
                      (map (fn [[k v]]
                             (:series v)))
                      cat)
                     summary-subset)
        legend (into []
                     (map (fn [[k v]]
                            (:legend v)))
                     summary-subset)
        data (into []
                   (map (fn [[k v]]
                          (:data v)))
                   summary-subset)]
    (when (seq data)
      (-> summary-items
          (merge chart-configuration)
          (assoc :sheet-name (or sheet-name chart-title))
          (assoc :data (apply tc/concat-copying
                              data))
          (assoc :chart
                 (-> {::series/series series
                      ::series/legend-spec legend}
                     (merge {::plot/legend-label legend-label
                             ::plot/title {::plot/label chart-title}}
                            base-chart-spec)
                     (plot/add-overview-legend-items)
                     chartf
                     (update ::plot/canvas plot/add-watermark watermark)))))))

(defn ->excel [charts {:keys [format-table-f
                              file-name]
                       :or {format-table-f format-table}}]
  (let [wb (-> (into []
                     (map (fn [{:keys [sheet-name chart data display-table]
                                :as _config}]
                            {::xl/sheet-name sheet-name
                             ::xl/images [{::xl/image (-> chart ::plot/canvas :buffer plot/->byte-array)}]
                             ::xl/data (if display-table
                                         display-table
                                         (format-table-f data))}))
                     charts)
               (xl/create-workbook))]
    (xl/save-workbook! wb file-name)
    wb))
