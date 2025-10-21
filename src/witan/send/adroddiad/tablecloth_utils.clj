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
(defn non-unique-by
  "Remove rows which contain unique data in the key `columns-selector` (default `:all`)
   columns, returning dataset containing only rows for which the key column
   values appear in more than one row (i.e. are non-unique).
   (Basically the opposite of `tc/unique-by`.)
   
   Specify `num-rows-same-colname` to have the row-count of the number of rows
   with the same values of the key columns in the dataset returned.
   
   Works within groups for grouped datasets."
  ([ds] (non-unique-by ds :all))
  ([ds columns-selector] (non-unique-by ds columns-selector nil))
  ([ds columns-selector & {:keys [num-rows-same-colname]
                           :or   {num-rows-same-colname :__num-rows-same}
                           :as   options}]
   (if (tc/grouped? ds)
     (tc/process-group-data ds #(non-unique-by % columns-selector options))
     (let [by-columns (tc/column-names ds columns-selector)]
       (-> ds
           (tc/group-by by-columns)
           (tc/add-column num-rows-same-colname tc/row-count)
           tc/ungroup
           (tc/select-rows #(-> % (get num-rows-same-colname) (> 1)))
           (tc/reorder-columns (concat (tc/column-names ds) [num-rows-same-colname]))
           (cond-> (not (:num-rows-same-colname options)) (tc/drop-columns num-rows-same-colname)))))))

(defn non-unique-by-keys
  "Return a row for each combination of key `columns-selector` (default `:all`) 
   column values that appear in more than one row of `ds` (i.e. are non-unique).
   
   Specify `num-rows-same-colname` to have the row-count of the number of rows
   with the same values of the key columns in the dataset returned.
   
   Works within groups for grouped datasets, but unless the grouping columns are
   included in the `columns-selector` (or option `include-grouping-columns` is
   specified truthy) they will not be retained in the individual group datasets.
   (Though they can be reinstated on ungrouping by using the 
   `:add-group-as-column` option of `tc/ungroup.)"
  ([ds] (non-unique-by-keys ds :all))
  ([ds columns-selector] (non-unique-by-keys ds columns-selector nil))
  ([ds columns-selector & {:keys [num-rows-same-colname
                                  include-grouping-columns]
                           :or   {num-rows-same-colname    :__num-rows-same
                                  include-grouping-columns false}
                           :as   options}]
   (let [by-columns (tc/column-names ds columns-selector)]
     
     (if (tc/grouped? ds)
       ;; Process each group, including the `grouping-columns` with the `by-columns` if requested
       (let [grouping-columns (->> ds :name (map keys) (apply concat) distinct)]
         (tc/process-group-data ds
                                #(non-unique-by-keys %
                                                     (concat
                                                      (when include-grouping-columns grouping-columns)
                                                      by-columns)
                                                     options)))
       (-> ds
           (tc/group-by by-columns)
           (tc/aggregate {num-rows-same-colname tc/row-count})
           (tc/select-rows #(-> % (get num-rows-same-colname) (> 1)))
           (tc/reorder-columns (concat (tc/column-names ds) [num-rows-same-colname]))
           (cond-> (not (:num-rows-same-colname options)) (tc/drop-columns num-rows-same-colname)))))))

(defn ^:deprecated select-non-unique-keys
  "Return unique combinations of `key-cols` that appear in more than one row of `ds`.
   DEPRECATED: Replaced by `select-non-unique-by`
               with option `{:num-rows-with-same-key-colname :num-rows}`."
  ([ds] (select-non-unique-keys ds (tc/column-names ds)))
  ([ds key-cols] (-> ds
                     (tc/group-by key-cols)
                     (tc/aggregate {:num-rows tc/row-count})
                     (tc/select-rows #(not= 1 (:num-rows %)))
                     (tc/reorder-columns (conj [:num-rows] key-cols)))))

(defn ^:deprecated select-non-unique-rows
  "Select non-unique rows of `ds`.
   DEPRECATED: Replaced by `non-unique-by`."
  [ds]
  (-> (tc/group-by ds (tc/column-names ds))
      (tc/add-column :num-rows-same tc/row-count)
      (tc/ungroup)
      (tc/select-rows #(not= 1 (:num-rows-same %)))
      (tc/drop-columns [:num-rows-same])))

(defn ^:deprecated select-non-unique-key-rows
  "Select rows of `ds` with non-unique values of `key-cols`.
   DEPRECATED: Replaced by `non-unique-by`."
  [ds key-cols]
  (-> ds
      (tc/group-by key-cols)
      (tc/add-column :num-rows-with-same-key tc/row-count)
      (tc/ungroup)
      (tc/select-rows #(not= 1 (:num-rows-with-same-key %)))
      (tc/drop-columns [:num-rows-with-same-key])))



;;; # dataset <-> map conversion
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

(defn key-comparator-fn-by
  "Returns function for ordering keys of a hash-map based on fn `key->order` 
   (mapping key to order) that is suitable for use in the specification
   of a custom sorted map via `sorted-map-by`.
   Per ClojureDocs example for `sorted-map-by` https://clojuredocs.org/clojure.core/sorted-map-by,
   the keys are included in the comparison to break ties where the `key-fn`
   does not return unique values for each key."
  [key->order]
  (fn [k1 k2]
    (compare [(key->order k1) k1]
             [(key->order k2) k2])))

(defn sorted-map-by-key-order
  "Given map `m` and function/map `key->order` that returns an ordering value
   for each key, returns a sorted map ordered by the mapped keys."
  [m key->order]
  (into (sorted-map-by (key-comparator-fn-by key->order)) m))

(defn ds->map
  "Given dataset `ds`, returns a (sorted) map with
   - keys from the `key-cols` of `ds`
   - vals from the remaining columns of `ds`
     or the `val-cols` if specified (which may overlap with `key-cols`)
   - ordered by the order of rows in the `ds`,
     or by the values of `order-col` if specified.
   The keys/vals in the returned map will be maps keyed by column name if
   `key-cols`/`key-vals` identify multiple columns or
   `single-key-col-as-map` (default `false`) or 
   `single-val-col-as-map` (default `true` ) are specified truthy, respectively.
   If `order-col` contains non-unique values then ties are broken using the `key-cols`."

  [ds & {:keys [key-cols single-key-col-as-map
                val-cols single-val-col-as-map
                order-col]
         :or   {single-key-col-as-map false
                single-val-col-as-map false}}]
  (let [all-cols (tc/column-names ds)
        key-cols (or key-cols (first all-cols))
        val-cols (or val-cols (remove (into #{} (tc/column-names ds key-cols)) all-cols))
        get-rows (fn [ds cols single-col-as-map]
                   (let [cols-ds (tc/select-columns ds cols)]
                     (if (or single-col-as-map (< 1 (tc/column-count cols-ds)))
                       (tc/rows cols-ds :as-maps)
                       (-> (tc/columns cols-ds :as-seqs) first))))
        ks       (get-rows ds key-cols single-key-col-as-map)
        vs       (get-rows ds val-cols single-val-col-as-map)
        os       (or (get ds order-col)
                     (range (tc/row-count ds)))]
    (sorted-map-by-key-order (zipmap ks vs)
                             (zipmap ks os))))

