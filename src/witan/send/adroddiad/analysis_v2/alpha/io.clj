(ns witan.send.adroddiad.analysis-v2.alpha.io
  "Functions for handling simulated transitions parquet files."
  ;; These used to be in `witan.send.adroddiad.summary-v2.io` (some with different names),
  ;; and were used by `witan.send.adroddiad.analysis.total-domain` from there
  ;; (i.e. there were no updated versions in `witan.send.adroddiad.analysis`).
  (:require
   [clojure.java.io :as io]
   [tech.v3.libs.parquet :as parquet]
   [ham-fisted.api :as hf]
   [ham-fisted.reduce :as hf-reduce]
   [tablecloth.api :as tc]))


(defn sorted-file-list [directory]
  "Returns sorted list of files in `directory`."
  (-> directory
      io/file
      .listFiles
      sort))

(defn simulated-transitions-filepaths
  "Given the path to the `directory` containing the simulated transitions parquet files,
   and the filename `prefix` of the simulated transitions to use,
   returns a sorted list of the filepaths to the simulated transitions files."
  [directory prefix]
  (into []
        (comp
         (filter #(re-find (re-pattern (format "%s-[0-9]+\\.parquet$" prefix)) (.getName %)))
         (map #(.getPath %)))
        (sorted-file-list directory)))

(defn read-and-split-simulated-transitions-parquet-file
  "Reads simulated transitions from a single parquet file at `filepath`,
   returning as a seq of datasets, one for each `:simulation`."
  [filepath]
  (-> filepath
      (parquet/parquet->ds {:key-fn keyword})
      (tc/group-by [:simulation] {:result-type :as-seq})))

(defn simulated-transitions-files->ds-vec
  "Given list of filepaths to simulated transitions parquet files,
   reads them, returning as a vector of datasets, one for each `:simulation`."
  [filepaths]
  (hf-reduce/preduce
   ;; init-val-fn
   (fn [] [])
   ;; rfn
   (fn [acc filepath]
     (into acc (read-and-split-simulated-transitions-parquet-file filepath)))
   ;; merge-fn
   (fn [m1 m2]
     (into m1 m2))
   {:min-n 2 :ordered? false}
   (hf/vec filepaths)))


