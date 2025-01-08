(ns witan.send.adroddiad.analysis-v2.alpha.summarise-domain
  (:require
   [clojure.java.io :as io]
   [tech.v3.libs.parquet :as parquet]
   [ham-fisted.api :as hf]
   [ham-fisted.reduce :as hf-reduce]
   [tablecloth.api :as tc]
   [tablecloth.column.api :as tcc]
   [tech.v3.dataset.reductions :as ds-reduce]
   [tech.v3.datatype.functional :as dfn]
   [witan.send :as ws]
   [witan.send.adroddiad.analysis-v2.alpha.io :as aio]
   #_[witan.send.adroddiad.transitions :as tr]))



;;; # File input for transitions (historic and simulated)
;;; ## Historic transitions
(defn count-transitions
  "Given a `transitions` dataset (with one record per CYP per `:calendar-year`),
   returns a `transitions-count` dataset with row counts in `:transition-count`."
  ;; TODO: Move to `witan.send.adroddiad.transitions`
  ;; TODO: Make a similar `sum-transition-counts` for summing existing `:transition-count`s (if needed)?
  [transitions]
  (-> transitions
      (tc/group-by [:calendar-year
                    :setting-1 :need-1 :academic-year-1
                    :setting-2 :need-2 :academic-year-2])
      (tc/aggregate {:transition-count tc/row-count})
      (tc/map-columns :calendar-year-2 [:calendar-year] (fn [^long cy] (inc cy)))))

(defn transitions-count-from-transitions-file
  "Given `filepath` to transitions CSV file (with one record per CYP per `:calendar-year`),
   reads the transitions and aggregates into a transitions-count dataset
    (one record per transition per `:calendar-year` with counts in `:transition-count`)."
  ;; TODO: Move to `witan.send.adroddiad.transitions`?
  [filepath]
  (-> filepath
      (tc/dataset {:key-fn keyword})
      count-transitions))


;;; ## Projections config handling
;; For getting parameters and file locations from projections model "config.edn" files.
(defn read-projections-config
  "Given path `config-edn-filepath` to projections config.edn file,
   and the `simulations-filename-prefix`,
   returns map read from the config.edn file by `witan.send/read-config`
   (which adds `:project-dir` to the both the base map and the map in the `:output-parameters` key)
   with key `:prefix` (containing the `simulations-filename-prefix`) added to the `:output-parameters` map."
  [config-edn-filepath simulations-filename-prefix]
  (-> config-edn-filepath
      (ws/read-config)
      (assoc-in [:output-parameters :prefix] simulations-filename-prefix)))

;;; ### Historic transitions
(defn historic-transitions-filepath-from-projections-config
  "Given `projections-config`, returns the path to the historic transitions file.
   The `projections-config` must contain keys `:project-dir` and `:file-inputs`, the latter containing `:transitions`
   (like when read from a projections config.edn by `witan.send/read-config`)."
  [{:keys                                    [project-dir]
    {historic-transitions-file :transitions} :file-inputs}]
  (str project-dir "/" historic-transitions-file))

(defn historic-transitions-count-from-projections-config
  "Given `projections-config`, returns the historic transitions count dataset.
   The `projections-config` must contain keys `:project-dir` and `:file-inputs`, the latter containing `:transitions`
   (like when read from a projections config.edn by `witan.send/read-config`)."
  [projections-config]
  (-> projections-config
      historic-transitions-filepath-from-projections-config
      transitions-count-from-transitions-file))

(comment ;; DEV
  @(def project-dir "./tmp/out-dir/")
  ;; TODO: remove
  @(def projections-config
     (-> project-dir
         (str "config-baseline-2023-2024.edn")
         (read-projections-config "baseline-2023-2024")))

  (-> projections-config
      historic-transitions-count-from-projections-config)

  (-> projections-config
      historic-transitions-count-from-projections-config
      :calendar-year
      distinct)

  )

;;; ### Simulated transitions from config
(defn simulated-transitions-filepaths-from-projections-config
  "Given `projections-config`, returns sorted list of filepaths to the simulated transition files.
   The `projections-config` must contain key `:output-parameters`,
   containing keys `:project-dir`, `:output-dir` and `:prefix`
   (like when read from a projections config.edn by `read-projections-config`)."
  [{{:keys [project-dir output-dir prefix]} :output-parameters}]
  (aio/simulated-transitions-filepaths (str project-dir "/" output-dir)
                                       prefix))

(defn simulated-transitions-from-projections-config
  "Given `projections-config`, returns vector of simulated transitions datasets, one dataset per `:simulation`.
   The `projections-config` must contain key `:output-parameters`,
   containing keys `:project-dir`, `:output-dir` and `:prefix`
   (like when read from a projections config.edn by `read-projections-config`)."
  [projections-config]
  (-> projections-config
      simulated-transitions-filepaths-from-projections-config
      aio/simulated-transitions-files->ds-vec))

(comment ;; DEV
  ;; TODO: remove
  (-> projections-config
      simulated-transitions-filepaths-from-projections-config)

  (def tmp
    (-> projections-config
        simulated-transitions-from-projections-config))

  (-> tmp
      count)

  (-> tmp
      first)

  (-> tmp
      first
      :calendar-year
      distinct)

  )



;;; # Functions for summarising
;; Replacement for add-diff (though returns previous value rather than calculating pct-diff):
;; See test ns `witan.send.adroddiad.analysis-v2.alpha.summarise-domain-test` for rationale and examples.
(defn add-sparse-lag1-diff-by-group
  "Given dataset `ds` containing (potentially sparse) time-series of values
   at (integer) indexed times, returns dataset with the lag 1 values and differences.
   If `group-key` is specified then lagging is done within group.
   The `temporal-index` should have integer increments be unique within the `group-key`.

   The returned dataset contains:
   - The grouping columns `group-key` (if specified).
   - The temporal index and value columns.
   - A column (added) containing the previous value.
   - Records (within each group) for all `temporal-index` values in the input dataset plus
     records for any in range `temporal-index` values for which the previous value is non-zero.
  - Any other columns in the input dataset (though will be `nil` for records added to fill gaps).

   Sparsity
   The input dataset `ds` may be sparse, i.e. omit records where the value is 0.
   In this case:
   - The range [`min-index`, `max-index`] (maths interval notation) of possible values
     for the `temporal-index`, unless specified, is taken from the input dataset (across all groups).
   - The value at a missing `temporal-index` that is within range is assumed to be 0.
   - The value at a `temporal-index` outside the range is assumed unknown (`nil`).
   - The output dataset will include records (within each group) for all `temporal-index` values
     in the input dataset, plus records for any in range `temporal-index` values for which the
     previous value was provided or otherwise known to be non-zero:
     - This may result in the output dataset having have more records than the input dataset,
       as the first `temporal-index` of a 'gap' will be filled in (since the previous value is
       known and the diff can be calculated).
     - This will include a record (within each group) for the `min-index`
       (with previous-value and therefore diff of nil), even if not in the input dataset.

   Options map/trailing-kv-pairs keys are as follows:
   - `:temporal-index-col` (default `:calendar-year`) - name of temporal index column.
   - `:value-col` (default `:transition-count`) - name of value column.
   - `:group-key` (default `nil`) - column-selector for grouping columns.
   - `:min-index` & `:max-index` (optional) - possible range of the `temporal-index-col` column
     (calculated from dataset if not provided).
   - `:previous-value-col` (optional) - name for previous value column in the returned dataset
     (defaults to names of the value column with '-previous' appended).
   - `:diff-col` (optional) - name for diff column in the returned dataset
     (defaults to names of the value column with '-diff' appended).
   - `:check-key-unique` (default `true`) - set to falsey to suppress checking
     that `:group-key` and `:temporal-index-col` together are a unique key
     (in the database sense) for `ds`.
  "
  [ds & {:keys [temporal-index-col value-col group-key
                min-index max-index
                previous-value-col diff-col
                check-index-range
                check-key-unique]
         :or   {temporal-index-col :calendar-year
                value-col          :count
                check-index-range  true
                check-key-unique   true}}]
  (let [min-ds-index       (-> ds temporal-index-col tcc/reduce-min)
        max-ds-index       (-> ds temporal-index-col tcc/reduce-max)
        _                  (when (and check-index-range
                                      (or (and (some? min-index)
                                               (> min-index min-ds-index))
                                          (and (some? max-index)
                                               (< max-index max-ds-index))))
                             (throw (ex-info (str "Input dataset temporal index range exceeds that specified.")
                                             {:temporal-index-col    temporal-index-col
                                              :index-range-in-ds     [min-ds-index max-ds-index]
                                              :index-range-specified [min-index    max-index]})))
        min-index          (or min-index min-ds-index)
        max-index          (or max-index max-ds-index)
        previous-value-col (or previous-value-col
                               (-> value-col name (str "-previous") keyword))
        diff-col           (or diff-col
                               (-> value-col name (str "-diff") keyword))
        group-key-cols     (tc/column-names ds group-key) ; parses any tc `column-selector` into seq of column names
        key-cols           (concat group-key-cols [temporal-index-col])]
    (when check-key-unique
      (let [non-unique-keys-ds (-> ds
                                   (tc/group-by key-cols)
                                   (tc/aggregate {:row-count tc/row-count})
                                   (tc/select-rows #(-> % :row-count (> 1))))]
        (when (< 0 (tc/row-count non-unique-keys-ds))
          (throw (ex-info (str "Input dataset has non-unique keys.")
                          {:group-key          group-key
                           :temporal-index-col temporal-index-col
                           :non-unique-keys-ds non-unique-keys-ds})))))
    (-> (tc/full-join (-> ds)
                      (-> ds
                          ;; Setup for previous years values
                          (tc/select-columns (conj key-cols value-col))
                          (tc/rename-columns {value-col previous-value-col})
                          (tc/update-columns temporal-index-col (partial map inc))
                          ;; Add (blank) record for `min-index` for each group (to ensure in final dataset)
                          ((fn [ds']
                             (tc/concat-copying (-> ds')
                                                (-> ds'
                                                    (#(if (seq group-key-cols) (tc/group-by % group-key-cols) %))
                                                    tc/first ; Take one record per group as template, doesn't matter which.
                                                    (tc/add-columns {temporal-index-col min-index
                                                                     previous-value-col nil})
                                                    (#(if (tc/grouped? %) (tc/ungroup %) %))))))
                          ;; Don't go beyond `max-index`
                          (tc/drop-rows #(-> % temporal-index-col (> max-index)))
                          (tc/set-dataset-name "right"))
                      key-cols)
        ;; Coalesce key-cols following full-join
        (tc/map-rows (fn [r]
                       (let [key-cols-right (map #(->> % name (str "right.") keyword) key-cols)]
                         (merge-with #(or %1 %2)
                                     (-> (select-keys r key-cols))
                                     (-> (select-keys r key-cols-right)
                                         (update-keys (zipmap key-cols-right key-cols)))))))
        (tc/drop-columns #"^:right\..+$")
        ;; Fill in missing values
        (tc/replace-missing [value-col] :value 0)
        (tc/map-columns previous-value-col [previous-value-col temporal-index-col]
                        #(or %1 (when (< min-index %2) 0)))
        ;; Calculate diffs
        (tc/map-columns diff-col [value-col previous-value-col] #(when %2 (- %1 %2)))
        ;; Arrange
        (tc/reorder-columns (concat key-cols [value-col previous-value-col diff-col]))
        (tc/order-by key-cols))))

