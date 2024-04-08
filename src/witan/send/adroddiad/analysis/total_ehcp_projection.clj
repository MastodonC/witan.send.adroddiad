(ns witan.send.adroddiad.analysis.total-ehcp-projection
  (:require
   [tech.v3.dataset.reductions :as ds-reduce]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.datatype.gradient :as gradient]
   [clojure.java.io :as io]
   [witan.send.adroddiad.transitions :as transitions]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [witan.population.england.snpp-2018 :as pop]
   [witan.send :as ws]
   [witan.send.adroddiad.summary-v2 :as summary]
   [witan.send.adroddiad.summary-v2.io :as sio]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]))

(defn transitions-from-config [config-edn]
  (let [config (ws/read-config config-edn)]
    (tc/dataset 
     (str (:project-dir config) "/" (get-in config [:file-inputs :transitions]))
     {:key-fn keyword})
    #_(-> (ws/build-input-datasets (:project-dir config) (:file-inputs config))
          :transitions
          tc/dataset)))

(defn historic-ehcp-count [transitions]
  (-> transitions
      (tc/group-by
       [:calendar-year
        :setting-1 :need-1 :academic-year-1
        :setting-2 :need-2 :academic-year-2])
      (tc/aggregate {:transition-count tc/row-count})
      (tc/map-columns :calendar-year-2 [:calendar-year] (fn [^long cy] (inc cy)))))

#_
(summary/summarise-census-by-keys )

(defn simulation-data-from-config [config-edn prefix]
  (->> config-edn
       ws/read-config
       :project-dir
       (sio/simulated-transitions-files prefix)
       (sio/files->ds-vec)))

