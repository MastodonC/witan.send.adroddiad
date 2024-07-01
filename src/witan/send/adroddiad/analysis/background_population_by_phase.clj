(ns witan.send.adroddiad.analysis.background-population-by-phase
  (:require
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [tech.v3.datatype.functional :as dfn]
   [witan.population.england.snpp-2018 :as pop]
   [witan.send :as ws]
   [witan.send.domain.academic-years :as day]
   [witan.send.adroddiad.summary-v2 :as summary]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]))

(defn sum-population-by-phase [population]
  (-> population
      (tc/map-columns :phase [:academic-year] day/ncy->school-phase-name)
      (tc/group-by [:calendar-year :phase])
      (tc/aggregate {:population #(dfn/sum (:population %))})))

(defn population-from-snpp [{:keys [la-name max-year start-age end-age]
                             :or {start-age 0
                                  end-age 25}
                             :as _conf}]
  (-> (pop/snpp-2018->witan-send-population
       @pop/snpp-2018-data
       {:la-name la-name :max-year max-year :start-age start-age :end-age end-age})
      sum-population-by-phase))

(defn population-from-config [config-edn]
  (let [config (ws/read-config config-edn)]
    (-> (ws/build-input-datasets (:project-dir config) (:file-inputs config))
        :population
        (tc/dataset)
        sum-population-by-phase)))

(def setting-phase-summary-defaults
  {:domain-key [:phase :setting]
   :extra-transformation-f
   (fn [ds]
     (tc/map-columns ds :phase [:academic-year] day/ncy->school-phase-name))})

(def phase-color-shapes
  (-> ["Nursery" "Primary" "Secondary" "Post 16" "Post 19"]
      (vs/color-and-shape-lookup)
      (tc/add-column :order (range))))

(def chart-base
  {:x           :calendar-year
   :x-title     "Calendar Year"
   :x-format    "Jan %Y"
   :y           :population
   :y-title     "Population"
   :y-format    "%,.0f"
   :y-zero      true
   :group       :phase
   :group-title "Phase"
   :chart-title "Population by Phase"
   :chart-width vs/two-thirds-width
   :chart-height vs/full-height})

(defn line-plot [{:keys [data group group-title colors-and-shapes
                         x x-title
                         y y-title y-format y-zero
                         chart-title chart-width chart-height]
                  :as chart-spec}]
  (vsl/line-plot
   (merge chart-base chart-spec)))

(defn plot
  ([population colors-and-shapes]
   (line-plot {:data (-> population
                         (tc/map-columns :calendar-year [:calendar-year] str))
               :colors-and-shapes colors-and-shapes}))
  ([population] (plot population phase-color-shapes)))

