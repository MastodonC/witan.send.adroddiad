(ns witan.send.adroddiad.simulated-transition-counts
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.reductions :as ds-reduce]
            [witan.send.adroddiad.simulated-transition-counts.io :as stcio]))

(defn transit->dataset [input-dir]
  (let [all-datasets (into []
                           (comp
                            stcio/gzipped-transit-file-paths-xf
                            (map (fn [file]
                                   (future
                                     (-> (into [] (stcio/transit-file->eduction file))
                                         tc/dataset)))))
                           (stcio/sorted-file-list input-dir))]
    (apply tc/concat-copying (map deref all-datasets))))

(defn full-simulation [simulation-input-dir historic-transitions-file-name]
  (let [simulation-data (transit->dataset simulation-input-dir)
        historic-data (-> (tc/dataset historic-transitions-file-name)
                          (tc/add-column "transition-count" 1)
                          (tc/add-column "simulation" -1)
                          (tc/rename-columns :all keyword))]
    (tc/concat-copying historic-data
                       simulation-data)))

(defn transition-counts->census-counts [transition-counts start-year]
  (let [year-1-census (-> transition-counts
                          (tc/select-rows #(= (:calendar-year %) start-year))
                          (tc/drop-columns [:setting-2 :need-2 :academic-year-2])
                          (tc/rename-columns {:setting-1 :setting
                                              :need-1 :need
                                              :academic-year-1 :academic-year}))]
    (-> transition-counts
        (tc/map-columns :calendar-year-2 [:calendar-year] #(inc %))
        (tc/drop-columns [:calendar-year :setting-1 :need-1 :academic-year-1])
        (tc/rename-columns {:calendar-year-2 :calendar-year
                            :setting-2 :setting
                            :need-2 :need
                            :academic-year-2 :academic-year})
        (tc/concat year-1-census)
        (tc/drop-rows #(= "NONSEND" (:setting %))))))
