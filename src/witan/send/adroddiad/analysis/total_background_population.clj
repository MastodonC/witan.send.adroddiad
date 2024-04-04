(ns witan.send.adroddiad.analysis.total-background-population
  (:require
   [tablecloth.api :as tc]
   [tech.v3.datatype.functional :as dfn]
   [witan.population.england.snpp-2018 :as pop]
   [witan.send :as ws]
   [witan.send.adroddiad.summary-v2 :as summary]
   [witan.send.adroddiad.vega-specs :as vs]))


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

;; Need to have a base year for increase/decrease
;; Need to calcuate diff and %-diff increase/decrease
(def chart-base
  {:x                 :calendar-year
   :x-title           "Calendar Year"
   :x-format          "%Y"
   :y                 :population
   :y-title           "Population"
   :y-format          "%,.2f"
   :y-zero            true
   :group             :age
   :group-title       "Age"
   :chart-width       vs/two-thirds-width
   ;; :colors-and-shapes (acc/color-and-shape-lookup (into (sorted-set) (:age population)))
   })
