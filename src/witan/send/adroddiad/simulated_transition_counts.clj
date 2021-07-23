(ns witan.send.adroddiad.simulated-transition-counts
  (:require [clojure.core.async :as a]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.transitions :as tr]
            [witan.send.adroddiad.simulated-transition-counts.io :as stcio]))

(def transit->dataset-xf
  (comp
   stcio/gzipped-transit-file-paths-xf
   (map (fn [file] (stcio/transit-file->eduction file)))
   (map (fn [e] (-> (into [] e)
                    (tc/dataset)
                    (tc/convert-types {:calendar-year :int16
                                       :academic-year-1 :int16
                                       :academic-year-2 :int16
                                       :simulation :int16
                                       :transition-count :int16}))))))

(defn historic-transitions [historic-transitions-file-name]
  (-> (tc/dataset historic-transitions-file-name)
      (tc/add-column "transition-count" 1)
      (tc/add-column "simulation" -1)
      (tc/rename-columns :all keyword)
      (tc/convert-types {:calendar-year :int16
                         :academic-year-1 :int16
                         :academic-year-2 :int16
                         :simulation :int16
                         :transition-count :int16})))

(defn full-simulation-sequence [input-dir historic-transitions-file-name]
  (sequence
   cat
   [[(historic-transitions historic-transitions-file-name)]
    (sequence
     transit->dataset-xf
     (stcio/sorted-file-list input-dir))]))

(defn transit->dataset-multi-threaded [input-dir]
  (let [concurrent (max 2 (quot (.. Runtime getRuntime availableProcessors) 2))
        output-chan (a/chan)]
    (a/pipeline-blocking concurrent
                         output-chan
                         transit->dataset-xf
                         (a/to-chan (stcio/sorted-file-list input-dir)))
    (apply tc/concat-copying (a/<!! (a/into [] output-chan)))))

(defn full-simulation-multi-threaded [simulation-input-dir historic-transitions-file-name]
  (let [simulation-data (transit->dataset-multi-threaded simulation-input-dir)
        historic-data (historic-transitions historic-transitions-file-name)]
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
