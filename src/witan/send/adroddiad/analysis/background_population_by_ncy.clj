(ns witan.send.adroddiad.analysis.background-population-by-ncy
  (:require
   [tablecloth.api :as tc]
   [witan.population.england.snpp-2018 :as pop]
   [witan.send :as ws]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]))

(defn population-from-snpp [{:keys [la-name max-year start-age end-age]
                             :or {start-age 0
                                  end-age 25}
                             :as _conf}]
  (-> (pop/snpp-2018->witan-send-population
       @pop/snpp-2018-data
       {:la-name la-name :max-year max-year :start-age start-age :end-age end-age})))

(defn population-from-config [config-edn]
  (let [config (ws/read-config config-edn)]
    (-> (ws/build-input-datasets (:project-dir config) (:file-inputs config))
        :population
        (tc/dataset))))

(def phase-color-shapes
  (-> (range 0 21)
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
   :group       :academic-year
   :group-title "NCY"
   :chart-title "Population by NCY"
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

