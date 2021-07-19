(ns witan.send.adroddiad.simulated-transition-counts
  (:require [clojure.core.async :as a]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.transitions :as tr]
            [witan.send.adroddiad.simulated-transition-counts.io :as stcio]))

(defn transit->dataset-multi-threaded [input-dir]
  (let [concurrent (max 2 (dec (.. Runtime getRuntime availableProcessors)))
        output-chan (a/chan)]
    (a/pipeline-blocking concurrent
                         output-chan
                         (comp
                          stcio/gzipped-transit-file-paths-xf
                          (map (fn [file] (stcio/transit-file->eduction file)))
                          (map (fn [e] (-> (into [] e)
                                           (tc/dataset)
                                           (tc/convert-types {:calendar-year :int16
                                                              :academic-year-1 :int16
                                                              :academic-year-2 :int16
                                                              :simulation :int16
                                                              :transition-count :int16})))))
                         (a/to-chan (stcio/sorted-file-list input-dir)))
    (apply tc/concat-copying (a/<!! (a/into [] output-chan)))))

(defn full-simulation-multi-threaded [simulation-input-dir historic-transitions-file-name]
  (let [simulation-data (transit->dataset-multi-threaded simulation-input-dir)
        historic-data (-> (tc/dataset historic-transitions-file-name)
                          (tc/add-column "transition-count" 1)
                          (tc/add-column "simulation" -1)
                          (tc/rename-columns :all keyword)
                          (tc/convert-types {:calendar-year :int16
                                             :academic-year-1 :int16
                                             :academic-year-2 :int16
                                             :simulation :int16
                                             :transition-count :int16}))]
    (tc/concat-copying historic-data
                       simulation-data)))

(defn transit->dataset [input-dir]
  (let [all-datasets (into []
                           (comp
                            stcio/gzipped-transit-file-paths-xf
                            (map (fn [file] (stcio/transit-file->eduction file)))
                            (map (fn [e] (tc/dataset (into [] e)))))
                           (stcio/sorted-file-list input-dir))]
    (apply tc/concat-copying all-datasets)))

(defn full-simulation [simulation-input-dir historic-transitions-file-name]
  (let [simulation-data (transit->dataset simulation-input-dir)
        historic-data (-> (tc/dataset historic-transitions-file-name)
                          (tc/add-column "transition-count" 1)
                          (tc/add-column "simulation" -1)
                          (tc/rename-columns :all keyword))]
    (tc/concat-copying historic-data
                       simulation-data)))

(def min-calendar-year tr/min-calendar-year)

(def transition-counts->census-counts tr/transitions->census)
