(ns witan.send.adroddiad.simulated-transition-counts.io
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [reducibles.core :as r]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transit Ingestion
(defn split-need-setting [need-setting]
  (let [need-setting-string (name need-setting)]
    (if (= need-setting-string "NONSEND")
      ["NONSEND" "NONSEND"]
      (s/split need-setting-string #"-"))))

(defn format-count-key [[calendar-year-2 academic-year-2 need-setting-1 need-setting-2]]
  (let [[need-1 setting-1] (split-need-setting need-setting-1)
        [need-2 setting-2] (split-need-setting need-setting-2)]
    {:calendar-year (dec calendar-year-2)
     :academic-year-1 (dec academic-year-2)
     :need-1 need-1
     :setting-1 setting-1
     :academic-year-2 academic-year-2
     :need-2 need-2
     :setting-2 setting-2}))


(defn simulated-transition->transition-count [sim-number [count-key transition-count]]
  (-> count-key
      format-count-key
      (assoc :simulation sim-number
             :transition-count transition-count)))

(def simulated-transitions->transition-counts-xf
  (comp
   (mapcat (fn [[simulation-number year-projections]]
             (map (fn [yp] [simulation-number yp])
                  year-projections)))
   (mapcat (fn [[simulation-number year-projection]]
             (map (fn [transition-count]
                    (simulated-transition->transition-count simulation-number transition-count))
                  year-projection)))))

(defn transit-file->eduction [filename]
  (eduction
   simulated-transitions->transition-counts-xf
   (r/transit-reducible :msgpack (-> filename
                                     io/file
                                     io/input-stream
                                     java.util.zip.GZIPInputStream.))))

(defn gzipped-transit-file-eduction [filename]
  (eduction
   (r/transit-reducible :msgpack (-> filename
                                     io/file
                                     io/input-stream
                                     java.util.zip.GZIPInputStream.))))

(def gzipped-transit-file-paths-xf
  (comp
   (filter #(re-find #"transit\.gz$" (.getName %)))
   (map #(.getPath %))))

(def transit-files-xf
  (comp
   gzipped-transit-file-paths-xf
   (map gzipped-transit-file-eduction)
   cat
   simulated-transitions->transition-counts-xf))

(defn sorted-file-list [directory]
  (-> directory
      io/file
      .listFiles
      sort))

(defn transit-files->eduction [directory]
  (eduction
   transit-files-xf
   (sorted-file-list directory)))
