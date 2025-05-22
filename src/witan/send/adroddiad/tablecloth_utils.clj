(ns witan.send.adroddiad.tablecloth-utils
  "Library of tablecloth utilities."
  (:require [tablecloth.api :as tc]))



;;; # Info
(defn ^:deprecated column-info
  "Selected column info, in column order.
   DEPRECATED: `tc/info` (since tc 7.x) returns column info in column order,
   so all this provides now is selection of info columns."
  [ds]
  (-> ds
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])))

(def column-info-col-name->label
  "Column info column labels for display"
  {:col-name           "Column Name"
   :datatype           "Data Type"
   :n-valid            "# Valid"
   :n-missing          "# Missing"
   :min                "Min"
   :mean               "Mean"
   :mode               "Mode"
   :max                "Max"
   :standard-deviation "Std. Dev."
   :skew               "Skew"
   :first              "First"
   :last               "Last"})

(defn column-info-with-labels
  "Selected column info, in column order, with labels."
  [ds ds-col-name->label]
  (-> ds
      tc/info
      (tc/map-columns :col-label [:col-name] ds-col-name->label)
      (tc/select-columns [:col-name :col-label :datatype :n-valid :n-missing :min :max])
      (tc/rename-columns (merge column-info-col-name->label {:col-label "Column Label"}))))



;;; # Non-unique
(defn select-non-unique-keys
  "Return unique combinations of `key-cols` that appear in more than one row of `ds`."
  ([ds] (select-non-unique-keys ds (tc/column-names ds)))
  ([ds key-cols] (-> ds
                     (tc/group-by key-cols)
                     (tc/aggregate {:num-rows tc/row-count})
                     (tc/select-rows #(not= 1 (:num-rows %)))
                     (tc/reorder-columns (conj [:num-rows] key-cols)))))

(defn select-non-unique-rows
  "Select non-unique rows of `ds`."
  [ds]
  (-> (tc/group-by ds (tc/column-names ds))
      (tc/add-column :num-rows-same tc/row-count)
      (tc/ungroup)
      (tc/select-rows #(not= 1 (:num-rows-same %)))
      (tc/drop-columns [:num-rows-same])))

(defn select-non-unique-key-rows
  "Select rows of `ds` with non-unique values of `key-cols`."
  [ds key-cols]
  (-> ds
      (tc/group-by key-cols)
      (tc/add-column :num-rows-with-same-key tc/row-count)
      (tc/ungroup)
      (tc/select-rows #(not= 1 (:num-rows-with-same-key %)))
      (tc/drop-columns [:num-rows-with-same-key])))



;;; # dataset <-> map conversion
(defn ds->hash-map
  "Given dataset `ds`, returns a hash-map with
   - keys from the `key-cols` of `ds`
   - vals from the remaining columns of `ds`
     or the `val-cols` if specified (which may overlap with `key-cols`),
   The keys/vals in the returned map will be maps keyed by column name if
   `key-cols`/`key-vals` identify multiple columns or
   `single-key-col-as-map` (default `false`) or 
   `single-val-col-as-map` (default `true`) are specified truthy, respectively."
  [ds & {:keys [key-cols single-key-col-as-map
                val-cols single-val-col-as-map]
         :or   {single-key-col-as-map false
                single-val-col-as-map false}}]
  (let [all-cols (tc/column-names ds)
        key-cols (or key-cols (first all-cols))
        val-cols (or val-cols (remove (into #{} (tc/column-names ds key-cols)) all-cols))
        get-rows (fn [ds cols single-col-as-map]
                   (let [cols-ds (tc/select-columns ds cols)]
                     (if (or single-col-as-map (< 1 (tc/column-count cols-ds)))
                       (tc/rows cols-ds :as-maps)
                       (-> (tc/columns cols-ds :as-seqs) first))))]
    (zipmap (get-rows ds key-cols single-key-col-as-map)
            (get-rows ds val-cols single-val-col-as-map))))

(defn compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(defn ds->sorted-map-by
  "Given dataset `ds`, returns a sorted-map with
   - keys from the `key-cols` of `ds`
   - vals from the remaining columns of `ds`
     or the `val-cols` if specified (which may overlap with `key-cols`)
   - ordered by the order of rows in the `ds`,
     or by the values of `:order-col` if specified (which must compare).
   The keys/vals in the returned map will be maps keyed by column name if
   `key-cols`/`key-vals` identify multiple columns or
   `single-key-col-as-map` (default `false`) or 
   `single-val-col-as-map` (default `true`) are specified truthy, respectively."
  [ds & {:keys [order-col]
         :as   opts}]
  (let [m  (ds->hash-map ds opts)
        o  (zipmap (keys m)
                   (or (get ds order-col)
                       (range)))]
    (into (sorted-map-by (partial compare-mapped-keys o))
          m)))

(defn map->ds
  "Given map `m`, returns dataset with keys and vals as columns.
   Column names via keyword parameters `:keys-col-name` & `:vals-col-name`.
   Keys or vals that are maps are expanded into columns named by the map keys."
  [m & {:keys [keys-col-name
               vals-col-name]
        :or   {keys-col-name :keys
               vals-col-name :vals}}]
  (->> m
       (reduce-kv (fn [vec-of-rows-as-maps k v]
                    (conj vec-of-rows-as-maps
                          (merge (if (map? k) k {keys-col-name k})
                                 (if (map? v) v {vals-col-name v}))))
                  [])
       tc/dataset))

