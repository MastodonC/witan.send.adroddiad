(ns witan.send.adroddiad.analysis-v2.alpha.summarise-domain-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.analysis-v2.alpha.summarise-domain :as asd]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.gradient :as gradient]))


;;; # add-sparse-lag1-diff-by-group
;;; ## Rationale
(comment
  ;; `add-sparse-lag1-diff-by-group` is a replacement for the old `add-diff`:
  (defn add-diff [ds value-col]
    (let [ds' (tc/order-by ds [:calendar-year])
          diff (gradient/diff1d (value-col ds'))
          values (value-col ds')
          pct-diff (sequence
                    (map (fn [d m] (if (zero? m)
                                     0
                                     (dfn// d m))))
                    diff values)]
      (-> ds'
          (tc/add-column :diff (into [0] diff))
          (tc/add-column :pct-diff (into [0] pct-diff)))))

  ;; designed to address the following issues:

  ;;; ### `add-diff` Issue #1: It assigns a `:diff` of 0 to the first year
  (-> (tc/dataset [[2021 1]]
                  {:column-names [:calendar-year :count]})
      (add-diff :count))
  ;; => :_unnamed [1 4]:
  ;;    | :calendar-year | :count | :diff | :pct-diff |
  ;;    |---------------:|-------:|------:|----------:|
  ;;    |           2021 |      1 |     0 |         0 |

  ;; …when it really should be treated as unknown (`nil`) and omitted from stats.

  ;;; ### `add-diff` Issue #2: add-diff jumps over missing `:calendar-year`s
  ;; With a continuous sequence of `:calendar-year`s (including years with 0 counts) it works:
  (-> (tc/dataset [[2021 1]
                   [2022 0]
                   [2023 0]
                   [2024 2]]
                  {:column-names [:calendar-year :count]})
      (add-diff :count))
  ;; => :_unnamed [4 4]:
  ;;    | :calendar-year | :count | :diff | :pct-diff |
  ;;    |---------------:|-------:|------:|----------:|
  ;;    |           2021 |      1 |     0 |         0 |
  ;;    |           2022 |      0 |    -1 |        -1 |
  ;;    |           2023 |      0 |     0 |         0 |
  ;;    |           2024 |      2 |     2 |         0 |

  ;; …but we don't get records for years with 0 counts when summarising transition records,
  ;;  resulting in the diff "jumping back" over a gap:
  ;; For example, here the diff for 2024 is from the 1 in 2021, not the implied 0 in 2023:
  (-> (tc/dataset [[2021 1]
                   [2024 2]]
                  {:column-names [:calendar-year :count]})
      (add-diff :count))


  ;;; ### `add-diff` Issue #3: Missing records for 1st `:calendar-year` of a gap
  ;; For the first year after a gap, we know the previous years count,
  ;; and the missing record count is 0, so we can calculate the diff (as -ve of previous value).
  ;; But add-diff only returns records for the `:calendar-year`s supplied.
  ;; For example, here, the `:diff` in 2022 is -1, but we don't get that:
  (-> (tc/dataset [[2021 1]
                   [2024 2]]
                  {:column-names [:calendar-year :count]})
      (add-diff :count))
  ;; => :_unnamed [2 4]:
  ;;    | :calendar-year | :count | :diff | :pct-diff |
  ;;    |---------------:|-------:|------:|----------:|
  ;;    |           2021 |      1 |     0 |         0 |
  ;;    |           2024 |      2 |     1 |         1 |

  ;;; ### `add-diff` Issue #4: `add-diff` must be applied separately for each group.
  ;; When we have multiple groups within a dataset, we have to `tc/group-by` and apply per group:
  (as-> (tc/dataset [["one" 2021 1 "(one)"]
                     ["one" 2022 0 "(one)"]
                     ["one" 2023 0 "(one)"]
                     ["one" 2024 2 "(one)"]
                     ["two" 2021 0 "(two)"]
                     ["two" 2022 3 "(two)"]
                     ["two" 2023 4 "(two)"]
                     ["two" 2024 0 "(two)"]]
                    {:column-names [:group :calendar-year :count :extra-col]}) $
    (tc/group-by $ :group {:result-type :as-seq})
    (map #(add-diff % :count) $)
    (apply tc/concat $))

  ;; …or (simpler, using `tc/process-group-data`)
  (-> (tc/dataset [["one" 2021 1 "(one)"]
                   ["one" 2022 0 "(one)"]
                   ["one" 2023 0 "(one)"]
                   ["one" 2024 2 "(one)"]
                   ["two" 2021 0 "(two)"]
                   ["two" 2022 3 "(two)"]
                   ["two" 2023 4 "(two)"]
                   ["two" 2024 0 "(two)"]]
                  {:column-names [:group :calendar-year :count :extra-col]})
      (tc/group-by :group)
      (tc/process-group-data #(add-diff % :count))
      tc/ungroup)
  ;; => _unnamed [8 6]:
  ;;    | :group | :calendar-year | :count | :extra-col | :diff | :pct-diff |
  ;;    |--------|---------------:|-------:|------------|------:|-----------|
  ;;    |    one |           2021 |      1 |      (one) |     0 |         0 |
  ;;    |    one |           2022 |      0 |      (one) |    -1 |        -1 |
  ;;    |    one |           2023 |      0 |      (one) |     0 |         0 |
  ;;    |    one |           2024 |      2 |      (one) |     2 |         0 |
  ;;    |    two |           2021 |      0 |      (two) |     0 |         0 |
  ;;    |    two |           2022 |      3 |      (two) |     3 |         0 |
  ;;    |    two |           2023 |      4 |      (two) |     1 |    0.3333 |
  ;;    |    two |           2024 |      0 |      (two) |    -4 |        -1 |

  ;; …because if we don't then the groups get mixed up and we get diffs between groups and within years:
  (-> (tc/dataset [["one" 2021 1 "(one)"]
                   ["one" 2022 0 "(one)"]
                   ["one" 2023 0 "(one)"]
                   ["one" 2024 2 "(one)"]
                   ["two" 2021 0 "(two)"]
                   ["two" 2022 3 "(two)"]
                   ["two" 2023 4 "(two)"]
                   ["two" 2024 0 "(two)"]]
                  {:column-names [:group :calendar-year :count :extra-col]})
      (add-diff :count))
  ;; => :_unnamed [8 6]:
  ;;    | :group | :calendar-year | :count | :extra-col | :diff | :pct-diff |
  ;;    |--------|---------------:|-------:|------------|------:|-----------|
  ;;    |    one |           2021 |      1 |      (one) |     0 |         0 |
  ;;    |    two |           2021 |      0 |      (two) |    -1 |        -1 |
  ;;    |    one |           2022 |      0 |      (one) |     0 |         0 |
  ;;    |    two |           2022 |      3 |      (two) |     3 |         0 |
  ;;    |    one |           2023 |      0 |      (one) |    -3 |        -1 |
  ;;    |    two |           2023 |      4 |      (two) |     4 |         0 |
  ;;    |    one |           2024 |      2 |      (one) |    -2 |   -0.5000 |
  ;;    |    two |           2024 |      0 |      (two) |    -2 |        -1 |

  )

(deftest add-sparse-lag1-diff-by-group
  (testing "Dataset including two consecutive zeros for 2022 & 2023, no groups."
    ;; Complete dataset version:
    (is (= (-> (tc/dataset [[2021 1]
                            [2022 0]
                            [2023 0]
                            [2024 2]]
                           {:column-names [:calendar-year :count]})
               asd/add-sparse-lag1-diff-by-group)
           (-> (tc/dataset [[2021 1 nil nil]
                            [2022 0   1  -1]
                            [2023 0   0   0]
                            [2024 2   0   2]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - Returned dataset has both the previous value and the diff.
           ))
    ;; Sparse version: Note: Have records for both "ends" of the range of `:calendar-year`.
    (is (= (-> (tc/dataset [[2021 1]
                            [2024 2]]
                           {:column-names [:calendar-year :count]})
               asd/add-sparse-lag1-diff-by-group)
           (-> (tc/dataset [[2021 1 nil nil]
                            [2022 0   1  -1]
                            [2024 2   0   2]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - Previous value for 2024 is assumed value of zero for 2023, not the value from the previous record (for 2021 with value 1).
           ;; - A record is added for 2022, since a record for the previous value was provided.
           ;; - A record is NOT added for 2023 as the input contained neither 2023 nor 2022:
           ;;   both value and previous value can be assumed 0.
           ;; - In general records will be added to fill the first years of any gaps in the input (which may include the first year).
           ))
    )

  (testing "Sparse with range greater than the dataset."
    ;; Complete dataset version:
    (is (= (-> (tc/dataset [[2020 0]
                            [2021 1]
                            [2022 0]]
                           {:column-names [:calendar-year :count]})
               asd/add-sparse-lag1-diff-by-group)
           (-> (tc/dataset [[2020 0 nil nil]
                            [2021 1   0   1]
                            [2022 0   1  -1]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))))
    ;; Sparse version but without specifying `min-index` or `max-index`:
    (is (= (-> (tc/dataset [[2021 1]]
                           {:column-names [:calendar-year :count]})
               asd/add-sparse-lag1-diff-by-group)
           (-> (tc/dataset [[2021 1 nil nil]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - Range for `:calendar-year` taken from input dataset, so only get a record for 2021, with `nil`s for previous & diff.
           ))
    ;; Sparse version but telling it `min-index` is 2020:
    (is (= (-> (tc/dataset [[2021 1]]
                           {:column-names [:calendar-year :count]})
               (asd/add-sparse-lag1-diff-by-group {:min-index 2020}))
           (-> (tc/dataset [[2020 0 nil nil]
                            [2021 1   0   1]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - Now the 2021 record has previous value 0 (since 2020 is a valid year), and a diff value.
           ;; - …and we also get a record for 2020 as first `:calendar-year`, with value assumed 0 (and `nil`s for previous and diff).
           ))
    ;; …and also telling it `max-index` is 2022:
    (is (= (-> (tc/dataset [[2021 1]]
                           {:column-names [:calendar-year :count]})
               (asd/add-sparse-lag1-diff-by-group {:min-index 2020
                                                   :max-index 2022}))
           (-> (tc/dataset [[2020 0 nil nil]
                            [2021 1   0   1]
                            [2022 0   1  -1]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - Now have a 2022 record as well, with assumed value 0 and previous value from 2021 with diff.
           ))
    )

  (testing "Grouping"
    ;; Complete dataset, grouping specified:
    (is (= (-> (tc/dataset [["one" 2021 1 "(one)"]
                            ["one" 2022 0 "(one)"]
                            ["one" 2023 0 "(one)"]
                            ["one" 2024 2 "(one)"]
                            ["two" 2021 0 "(two)"]
                            ["two" 2022 3 "(two)"]
                            ["two" 2023 4 "(two)"]
                            ["two" 2024 0 "(two)"]]
                           {:column-names [:group :calendar-year :count :extra-col]})
               (asd/add-sparse-lag1-diff-by-group {:group-key :group}))
           (-> (tc/dataset [["one" 2021 1 nil nil "(one)"]
                            ["one" 2022 0   1  -1 "(one)"]
                            ["one" 2023 0   0   0 "(one)"]
                            ["one" 2024 2   0   2 "(one)"]
                            ["two" 2021 0 nil nil "(two)"]
                            ["two" 2022 3   0   3 "(two)"]
                            ["two" 2023 4   3   1 "(two)"]
                            ["two" 2024 0   4  -4 "(two)"]]
                           {:column-names [:group :calendar-year :count :count-previous :count-diff :extra-col]}))
           ;; Note:
           ;; - Lagging is within group.
           ;; - Grouping columns are moved to the start of the dataset.
           ;; - Additional columns (`:extra-col` in this case) are moved to the end.
           ))
    ;; Sparse version, grouping specified:
    (is (= (-> (tc/dataset [["one" 2021 1 "(one)"]
                            ["one" 2024 2 "(one)"]
                            ["two" 2022 3 "(two)"]
                            ["two" 2023 4 "(two)"]]
                           {:column-names [:group :calendar-year :count :extra-col]})
               (asd/add-sparse-lag1-diff-by-group {:group-key :group}))
           (-> (tc/dataset [["one" 2021 1 nil nil "(one)"]
                            ["one" 2022 0   1  -1     nil]
                            ["one" 2024 2   0   2 "(one)"]
                            ["two" 2021 0 nil nil     nil]
                            ["two" 2022 3   0   3 "(two)"]
                            ["two" 2023 4   3   1 "(two)"]
                            ["two" 2024 0   4  -4     nil]]
                           {:column-names [:group :calendar-year :count :count-previous :count-diff :extra-col]}))
           ;; Note:
           ;; - Although `:calendar-year` is itself a unique key for the input dataset, lagging is done within group,
           ;;   so the group "two" 2023 record (with value 4) is not the previous record for the group "one" 2024 record.
           ;; - In the rows added (to fill "gaps" where previous value was provided or known non-zero):
           ;;   - The values of the grouping columns are filled in (as known for the group).
           ;;   - But the value for any additional columns (`:extra-col` here) are missing (as not provided in input dataset).
           ;; - The temporal range (for `:calendar-year`):
           ;;   - In the absence of specification via `:min-index` & `:max-index`,
           ;;     is taken across the entire input dataset.
           ;;   - Here this yields min-index of 2021 and max-index of 2024 (both from group "one").
           ;;   - The range is applied to each group.
           ;;   - So in this example, whilst the 2021--2024 range comes from group "one",
           ;;     it's application to group "two" results in added records for both 2021 & 2024 in group "two",
           ;;     the latter since we have a 2023 value to diff against and max-index of 2024 tells us that
           ;;     2024 is a valid index for which the value is assumed 0 as not in the input dataset.
           ))
    )

  (testing "Non-unique keys"
    ;; The dataset must have at most one temporal index value per group.
    ;; I.e. the `temporal-index` column and `group-key` columns together must form a unique key for the dataset.
    ;; This is checked, and an exception thrown if any keys are not unique.
    (is (= (try
             (-> (tc/dataset [[2021 1]
                              [2022 2]
                              [2022 3]]
                             {:column-names [:calendar-year :count]})
                 asd/add-sparse-lag1-diff-by-group)
             (catch Exception e (str "Caught exception: " (.getMessage e))))
           "Caught exception: Input dataset has non-unique keys."
           ;; Note:
           ;; - Lagging is within group.
           ;; - Grouping columns are moved to the start of the dataset.
           ;; - Additional columns (`:extra-col` in this case) are moved to the end.
           ))
    ;; …but you can suppress the checking by specifying a falsey value for `:check-key-unique`.
    ;; …and if you do that with a non-unique key you'll get a lot of records back because
    ;;  of the full-join of the dataset with it's lagged version:
    (is (= (-> (tc/dataset [[2021 1]
                            [2021 2]
                            [2022 3]]
                           {:column-names [:calendar-year :count]})
               (asd/add-sparse-lag1-diff-by-group {:check-key-unique false}))
           (-> (tc/dataset [[2021 1 nil nil]
                            [2021 2 nil nil]
                            [2022 3   1   2]
                            [2022 3   2   1]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - No exception is thrown because `{:check-key-unique false}` is specified.
           ;; - There are 2 2021 records that can provide the previous value for the 2022 record,
           ;;   so there are 2 2022 records in the output dataset.
           ))
    )

  (testing "Specified temporal index range exceeded in the dataset"
    ;; If the temporal index range min and/or max are specified then they must not be exceeded in the dataset.
    ;; An exception is thrown if a temporal index max or min are specified that are exceeded in the dataset:
    (is (= (try (-> (tc/dataset [[2021 1]
                                 [2022 2]
                                 [2023 3]
                                 [2024 4]]
                                {:column-names [:calendar-year :count]})
                    (asd/add-sparse-lag1-diff-by-group {:min-index 2022}))
                (catch Exception e (str "Caught exception: " (.getMessage e))))
           "Caught exception: Input dataset temporal index range exceeds that specified."
           ))
    (is (= (try (-> (tc/dataset [[2021 1]
                                 [2022 2]
                                 [2023 3]
                                 [2024 4]]
                                {:column-names [:calendar-year :count]})
                    (asd/add-sparse-lag1-diff-by-group {:max-index 2023}))
                (catch Exception e (str "Caught exception: " (.getMessage e))))
           "Caught exception: Input dataset temporal index range exceeds that specified."
           ;; Note:
           ;; - No exception is thrown because `{:check-key-unique false}` is specified.
           ;; - There are 2 2021 records that can provide the previous value for the 2022 record,
           ;;   so there are 2 2022 records in the output dataset.
           ))
    ;; …but you can suppress the checking by specifying a falsey value for `:check-index-range`.
    ;; …and if you do that with an index range smaller than that of the dataset
    ;;  you'll get some strange results:
    (is (= (-> (tc/dataset [[2021 1]
                            [2022 2]
                            [2023 3]
                            [2024 4]]
                           {:column-names [:calendar-year :count]})
               (asd/add-sparse-lag1-diff-by-group {:min-index         2022
                                                   :max-index         2023
                                                   :check-index-range false}))
           (-> (tc/dataset [[2021 1 nil nil]
                            [2022 2 nil nil]
                            [2022 2   1   1]
                            [2023 3   2   1]
                            [2024 4   0   4]]
                           {:column-names [:calendar-year :count :count-previous :count-diff]}))
           ;; Note:
           ;; - There are two records for the `:min-index` year of 2022:
           ;;   - The first is added for the (erroneous) `:min-index` year
           ;;     of 2022 with missing previous value.
           ;;   - The second is from the (correct) combination of the
           ;;     2022 record supplied with the lagged 2021 record supplied.
           ;; - The previous value for 2024 has been assumed 0 (rather than the value 3 from the 2023 record),
           ;;   because the algorithm doesn't lag beyond the specified `:max-index` of 2023.
           ))
    )

  )

(comment
  (clojure.test/run-tests)

  )
