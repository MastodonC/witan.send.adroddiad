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
;; Replacement for add-diff to cope with sparse input:
;; See test ns `witan.send.adroddiad.analysis-v2.alpha.summarise-domain-test` for rationale and examples.
(defn add-previous-year-diff-to-sparse
  "Given dataset `ds` containing (potentially sparse) time-series at (integer) `:calendar-year`s
   of values (in column `value-col`), returns a dataset for all years with the value,
   previous years value and differences.

   To permit use within groups, if `group-cols` are specified then corresponding
   columns are also included in the returned dataset with values taken from the
   first row of `ds`.

   Sparsity
   The input dataset `ds` may be sparse, i.e. omit `:calendar-year`s where the value is 0.
   In this case:
   - The range [`min-cy`, `max-cy`] (maths interval notation) of possible `:calendar-year`s,
     unless specified, is taken from the input dataset.
   - The value at a `:calendar-year` that is within range but not in `ds` is assumed to be 0.
   - The value at a `:calendar-year` outside the specified range is assumed unknown (`nil`),
     even if a value was provided in the input dataset `ds`.
   - The output dataset will include a record for all `:calendar-year`s in the range.
     (Thus, given sparse input, the output dataset will have additional rows.)

  Trailing options map/kv-pairs keys are as follows:
   - `value-col` (default `:transition-count`) - name of value column.
   - `min-cy` & `max-cy` (optional) - range of `:calendar-year`s to consider
     (each calculated from the dataset if not provided).
   - `previous-value-col` (optional) - name for previous value column in the returned dataset
     (defaults to names of the value column with '-previous' appended).
   - `diff-col` (optional) - name for diff column in the returned dataset
     (defaults to names of the value column with '-diff' appended).
   - `group-cols` (default `nil`) - column-selector for grouping columns.

  Note:
  - Due to handling sparsity, no other columns of the input dataset `ds` are returned.
  - The `:calendar-year` is used as an index and therefore must be unique.
  - Any `:calendar-year`s outside any range specified are ignored.
  - The previous value and diff for the `min-cy` `:calendar-year` are returned as `nil`.
  "
  [ds & {:keys [value-col min-cy max-cy previous-value-col diff-col group-cols]
         :or   {value-col :count}}]
  (let [min-cy             (or min-cy (-> ds :calendar-year tcc/reduce-min))
        max-cy             (or max-cy (-> ds :calendar-year tcc/reduce-max))
        previous-value-col (or previous-value-col (-> value-col name (str "-previous") keyword))
        diff-col           (or diff-col           (-> value-col name (str "-diff")     keyword))
        cy->value          (zipmap (ds :calendar-year) (ds value-col))
        group-cols-row     (-> ds (tc/select-columns group-cols) (tc/rows :as-maps) first)]
    (when (< (count cy->value) (tc/row-count ds))
      (throw (ex-info (str "Some `:calendar-year`s repeated in input dataset.")
                      {:ds ds, :calendar-years (->> ds :calendar-year frequencies (filter #(-> % second (> 1))) keys)})))
    (-> (tc/dataset {:calendar-year (range min-cy (inc max-cy))})
        (tc/map-columns value-col [:calendar-year] #(get cy->value % 0))
        (tc/add-column previous-value-col #(into [nil] (-> % value-col butlast)))
        (tc/map-columns diff-col [value-col previous-value-col] #(when %2 (- %1 %2)))
        (tc/map-rows (constantly group-cols-row))
        (tc/reorder-columns group-cols))))

(defn add-previous-year-diff-to-sparse-by-group
  "A wrapper for `add-previous-year-diff` for grouped data:
   Groups `ds` by the `group-cols`, calls `add-previous-year-diff` for each and ungroups.
   Trailing argument options map/kv-pairs are as for `add-previous-year-diff`,
   but are parsed here for defaults to avoid repetition and so that default
   `min-cy` & `max-cy` are calculated over all groups."
  [ds & {:keys [value-col min-cy max-cy previous-value-col diff-col group-cols]}]
  (let [value-col          (or value-col :count)
        min-cy             (or min-cy (-> ds :calendar-year tcc/reduce-min))
        max-cy             (or max-cy (-> ds :calendar-year tcc/reduce-max))
        previous-value-col (or previous-value-col
                               (-> value-col name (str "-previous") keyword))
        diff-col           (or diff-col
                               (-> value-col name (str "-diff") keyword))
        group-cols         (tc/column-names ds group-cols)]
    (-> ds
        (tc/group-by group-cols)
        (tc/process-group-data
         #(add-previous-year-diff-to-sparse % {:value-col          value-col
                                               :min-cy             min-cy
                                               :max-cy             max-cy
                                               :previous-value-col previous-value-col
                                               :diff-col           diff-col
                                               :group-cols         group-cols}))
        tc/ungroup)))

