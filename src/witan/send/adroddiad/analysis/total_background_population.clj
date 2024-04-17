(ns witan.send.adroddiad.analysis.total-background-population
  (:require
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [tech.v3.datatype.functional :as dfn]
   [witan.population.england.snpp-2018 :as pop]
   [witan.send :as ws]
   [witan.send.adroddiad.summary-v2 :as summary]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]))


(defn population-from-snpp [{:keys [la-name max-year start-age end-age]
                             :or {start-age 0
                                  end-age 25}
                             :as _conf}]
  (-> (pop/snpp-2018->witan-send-population
       @pop/snpp-2018-data
       {:la-name la-name :max-year max-year :start-age start-age :end-age end-age})
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population #(dfn/sum (:population %))})))

(defn population-from-config [config-edn]
  (let [config (ws/read-config config-edn)]
    (-> (ws/build-input-datasets (:project-dir config) (:file-inputs config))
        :population
        (tc/dataset)
        (tc/group-by [:calendar-year])
        (tc/aggregate {:population #(dfn/sum (:population %))}))))

(defn summarise-population [pop]
  (-> pop
      (summary/add-diff :population)))

(defn summarise-population-trend [pop {:keys [anchor-year]}]
  (let [anchor-year (or anchor-year (reduce min (:calendar-year pop)))
        five-year (+ 5 anchor-year)
        ten-year (+ 10 anchor-year)
        anchor-year-pop (summary/value-at pop #(= anchor-year (:calendar-year %)) :population)
        five-year-pop (summary/value-at pop #(= five-year (:calendar-year %)) :population)
        ten-year-pop (summary/value-at pop #(= ten-year (:calendar-year %)) :population)
        five-year-delta (dfn/- five-year-pop anchor-year-pop)
        five-year-delta-pct (float (dfn// five-year-delta anchor-year-pop))
        ten-year-delta (dfn/- ten-year-pop anchor-year-pop)
        ten-year-delta-pct (float (dfn// ten-year-delta anchor-year-pop))]
    {:anchor-year anchor-year
     :five-year five-year
     :ten-year ten-year
     :anchor-year-pop anchor-year-pop
     :five-year-pop five-year-pop
     :ten-year-pop ten-year-pop
     :five-year-delta five-year-delta
     :five-year-delta-pct five-year-delta-pct
     :ten-year-delta ten-year-delta
     :ten-year-delta-pct ten-year-delta-pct}))

(defn population-trend-description
  [{:keys [anchor-year five-year ten-year anchor-year-pop five-year-pop ten-year-pop five-year-delta five-year-delta-pct ten-year-delta ten-year-delta-pct]}]
  [(format
    (if (pos? five-year-delta)
      "The 0-25 population in %d of %,.0f is expected to go up by %,.0f to %,.0f by the year %d, which is an increase of %,.1f%%."
      "The 0-25 population in %d of %,.0f is expected to go down by %,.0f to %,.0f by the year %d, which is a decrease of %,.1f%%.")
    anchor-year anchor-year-pop (* -1 five-year-delta) five-year-pop five-year (* -100 five-year-delta-pct))
   (format
    (if (pos? ten-year-delta)
      "By %d it will have gone up by %,.0f to %,.0f, which is an increase of %,.1f%% over 10 years."
      "By %d it will have gone down by %,.0f to %,.0f, which is a decrease of %,.1f%% over 10 years.")
    ten-year (* -1 ten-year-delta) ten-year-pop (* -100 ten-year-delta-pct))])

(defn population-trend-headline
  [{:keys [anchor-year five-year five-year-delta five-year-delta-pct]}]
  (if (pos? five-year-delta)
    (format "Between %d-%d 0-25 Population Expected to Grow by %,.1f%%" anchor-year five-year (* -100 five-year-delta-pct))
    (format "Between %d-%d 0-25 Population Expected to Shrink by %,.1f%%" anchor-year five-year (* -100 five-year-delta-pct))))

(def chart-base
  {:x           :calendar-year
   :x-title     "Calendar Year"
   :x-format    "Jan %Y"
   :y           :population
   :y-title     "Population"
   :y-format    "%,.0f"
   :y-zero      true
   :group       :source
   :group-title "Source"
   :chart-title "0-25 Population"
   :chart-width vs/two-thirds-width
   :chart-height vs/full-height})

(defn line-plot [{:keys [data group group-title colors-and-shapes
                         x x-title
                         y y-title y-format y-zero
                         chart-title chart-width chart-height] 
                  :as chart-spec}]
  (vsl/line-plot
   (merge chart-base chart-spec)))

(defn summary-charts-and-data
  [population {:keys [anchor-year colors-and-shapes source]
               :or {source "ONS"}}]
  (let [population' (-> population
                        (tc/add-column :source source)
                        (summarise-population))
        summary (summarise-population-trend population' {:anchor-year anchor-year})
        plot (line-plot {:data (-> population'
                                   (tc/map-columns :calendar-year [:calendar-year] str))
                         :colors-and-shapes colors-and-shapes})
        headline (population-trend-headline summary)
        description (population-trend-description summary)]
    {:data population'
     :summary summary
     :plot plot
     :headline headline
     :description description
     :fragment (clerk/fragment
                (clerk/html
                 {::clerk/width :full}
                 [:h2 headline])
                (clerk/row
                 {::clerk/width :full}
                 (clerk/vl plot)
                 (clerk/html (into [:div.max-w-screen-2xl.font-sans]
                                   (mapv (fn [d] [:p.mb-5 d]) description)))))}))
