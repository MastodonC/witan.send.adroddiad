(ns witan.send.adroddiad.summary-v2.io
  (:require
   [clojure.java.io :as io]
   [tech.v3.libs.parquet :as parquet]
   [tablecloth.api :as tc]
   [ham-fisted.reduce :as hf-reduce]
   [ham-fisted.api :as hf]))

(defn sorted-file-list [directory]
  (-> directory
      io/file
      .listFiles
      sort))

(defn simulated-transitions-files [prefix directory]
  (into []
        (comp
         (filter #(re-find (re-pattern (format "%s-[0-9]+\\.parquet$" prefix)) (.getName %)))
         (map #(.getPath %)))
        (sorted-file-list directory)))

(defn read-and-split [ds-split-key file]
  (-> file
      (parquet/parquet->ds {:key-fn keyword})
      (tc/group-by [ds-split-key] {:result-type :as-seq})))

(defn files->ds-vec [file-names]
  (hf-reduce/preduce
   ;; init-val-fn
   (fn [] [])
   ;; rfn
   (fn [acc pqt-file]
     (into acc (read-and-split :simulation pqt-file)))
   ;; merge-fn
   (fn [m1 m2]
     (into m1 m2))
   {:min-n 2 :ordered? false}
   (hf/vec file-names)))

