(ns witan.send.adroddiad.tablecloth-utils
  "Library of tablecloth utilities."
  (:require [tablecloth.api :as tc]))

(defn column-info
  "Selected column info, in column order"
  [ds]
  (let [column-name->order (zipmap (tc/column-names ds) (iterate inc 1))]
    (-> ds
        (tc/info)
        (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
        (tc/order-by #(column-name->order (:col-name %))))))

(def column-info-col-name->label
  "Column info column labels for display"
  {:col-name  "Column Name"
   :datatype  "Data Type"
   :n-valid   "# Valid"
   :n-missing "# Missing"
   :min       "Min"
   :max       "Max"})

(defn column-info-with-labels
  "Selected column info, in column order, with labels."
  [ds ds-col-name->label]
  (-> ds
      column-info
      (tc/map-columns :col-label [:col-name] ds-col-name->label)
      (tc/reorder-columns [:col-name :col-label])
      (tc/rename-columns (merge column-info-col-name->label {:col-label "Column Label"}))))

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

