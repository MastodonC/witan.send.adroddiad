(ns witan.send.adroddiad.analysis-v2.alpha.summarise-domain-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.analysis-v2.alpha.summarise-domain :as asd]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.gradient :as gradient]))


;;; # add-diff replacement
(comment
  ;;; ## Issues with old `add-diff`
  ;; The current `add-diff`:
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

  ;; has some issues:

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

(deftest add-previous-year-diff-to-sparse
  (testing "Basic derivation of previous value and diff for dataset with all `:calendar-year`s."
    (is (=
         ;; Note: Zero `:count`s in 2022 & 2023.
         (-> (tc/dataset [[2021 1]
                          [2022 0]
                          [2023 0]
                          [2024 2]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         ;; Note:
         ;; - Returned dataset has both the previous value and the diff.
         ;; - Previous value and diff for 1st `:calendar-year` are `nil`.
         (-> (tc/dataset [[2021 1 nil nil]
                          [2022 0   1  -1]
                          [2023 0   0   0]
                          [2024 2   0   2]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]})))))

  (testing "Previous value and diff correct for sparse input with missing `:calendar-year`s."
    (is (=
         ;; Note: Have records for both "ends" of the range of `:calendar-year`.
         (-> (tc/dataset [[2021 1]
                          [2024 2]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         ;; Note:
         ;; - Previous value for 2024 is assumed value of zero for 2023, not the value from the previous record (for 2021 with value 1).
         ;; - Returned dataset has all `:calendar-year`s in range.
         (-> (tc/dataset [[2021 1 nil nil]
                          [2022 0   1  -1]
                          [2023 0   0   0]
                          [2024 2   0   2]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]})))))

  (testing "Input dataset does not need to be sorted by `:calendar-year`"
    ;; Complete version
    (is (=
         ;; Note: Not sorted
         (-> (tc/dataset [[2022 0]
                          [2024 2]
                          [2021 1]
                          [2023 0]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         ;; Note: Sorted
         (-> (tc/dataset [[2021 1 nil nil]
                          [2022 0   1  -1]
                          [2023 0   0   0]
                          [2024 2   0   2]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]}))))
    ;; Sparse version
    (is (=
         ;; Note: Not sorted
         (-> (tc/dataset [[2024 2]
                          [2021 1]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         ;; Note: Sorted
         (-> (tc/dataset [[2021 1 nil nil]
                          [2022 0   1  -1]
                          [2023 0   0   0]
                          [2024 2   0   2]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]})))))

  (testing "Sparse with specified range for `:calendar-year` greater than in the dataset."
    ;; Complete dataset version:
    (is (=
         (-> (tc/dataset [[2020 0]
                          [2021 1]
                          [2022 0]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         (-> (tc/dataset [[2020 0 nil nil]
                          [2021 1   0   1]
                          [2022 0   1  -1]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]}))))
    ;; Sparse version but without specifying `min-cy` or `max-cy`:
    (is (=
         (-> (tc/dataset [[2021 1]]
                         {:column-names [:calendar-year :count]})
             asd/add-previous-year-diff-to-sparse)
         ;; Note: Range for `:calendar-year` taken from input dataset, so only get a record for 2021, with `nil`s for previous & diff.
         (-> (tc/dataset [[2021 1 nil nil]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]}))))
    ;; Sparse version but telling it `min-index` is 2020:
    (is (=
         ;; Note: `min-cy` is specified as 2020, whereas 2021 is min in dataset.
         (-> (tc/dataset [[2021 1]]
                         {:column-names [:calendar-year :count]})
             (asd/add-previous-year-diff-to-sparse {:min-cy 2020}))
         ;; Note:
         ;; - Now have a 2020 record (with `nil` previous value and diff).
         ;; - The 2021 record now has previous value 0 (since 2020 is a valid year), and a diff value.
         (-> (tc/dataset [[2020 0 nil nil]
                          [2021 1   0   1]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]}))))
    ;; …and also telling it `max-cy` is 2022:
    (is (=
         ;; Note: `max-cy` is specified as 2022, whereas 2021 is max in dataset.
         (-> (tc/dataset [[2021 1]]
                         {:column-names [:calendar-year :count]})
             (asd/add-previous-year-diff-to-sparse {:min-cy 2020
                                                    :max-cy 2022}))
         ;; Note: Now have a 2022 record as well, with assumed value 0 and previous value from 2021 with diff.
         (-> (tc/dataset [[2020 0 nil nil]
                          [2021 1   0   1]
                          [2022 0   1  -1]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]})))))

  (testing "Sparse with specified range for `:calendar-year` smaller than in the dataset."
    ;; Records for `:calendar-year`s out of range are ignored
    (is (=
         ;; Note: Dataset contains 2021--2024 but we've specified `min-cy` as 2022 & `max-cy` as 2023
         (-> (tc/dataset [[2021 1]
                          [2022 0]
                          [2023 0]
                          [2024 2]]
                         {:column-names [:calendar-year :count]})
             (asd/add-previous-year-diff-to-sparse {:min-cy 2022
                                                    :max-cy 2023}))
         ;; Note:
         ;; - Only get records for 2022--2023.
         ;; - Since it considers 2022 the `min-cy` and ignores records with smaller `:calendar-year`s
         ;;   the previous value and diff for the 2022 record are `nil`.
         (-> (tc/dataset [[2022 0 nil nil]
                          [2023 0  0    0]]
                         {:column-names [:calendar-year :count :count-previous :count-diff]})))))

  (testing "Grouping and other columns"
    ;; To facilitate use within `tc/group-by` groups,
    ;; columns specified as `group-cols` are included in the output dataset:
    (is (=
         ;; Note: Column `:group` specified as `group-cols`
         (-> (tc/dataset [[2020 0 "two"]
                          [2021 1 "two"]
                          [2022 0 "two"]]
                         {:column-names [:calendar-year :count :group]})
             (asd/add-previous-year-diff-to-sparse {:group-cols :group}))
         ;; Note: The group columns are the first columns in the returned dataset.
         (-> (tc/dataset [["two" 2020 0 nil nil]
                          ["two" 2021 1   0   1]
                          ["two" 2022 0   1  -1]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]}))))
    ;; …and can specify multiple columns:
    (is (=
         ;; Note: `group-cols` specified as `[:group-col-1 :group-col-2]`
         (-> (tc/dataset [[2020 0 "two" "a"]
                          [2021 1 "two" "a"]
                          [2022 0 "two" "a"]]
                         {:column-names [:calendar-year :count :group-col-1 :group-col-2]})
             (asd/add-previous-year-diff-to-sparse {:group-cols [:group-col-1 :group-col-2]}))
         ;; Note: The group columns are the first columns in the returned dataset.
         (-> (tc/dataset [["two" "a" 2020 0 nil nil]
                          ["two" "a" 2021 1   0   1]
                          ["two" "a" 2022 0   1  -1]]
                         {:column-names [:group-col-1 :group-col-2 :calendar-year :count :count-previous :count-diff]}))))
    ;; …using any valid tablecloth `column-selector`
    (is (=
         ;; Note: `group-cols` specified via regex
         (-> (tc/dataset [[2020 0 "two" "a"]
                          [2021 1 "two" "a"]
                          [2022 0 "two" "a"]]
                         {:column-names [:calendar-year :count :group-col-1 :group-col-2]})
             (asd/add-previous-year-diff-to-sparse {:group-cols #"^:group-col-\d$"}))
         ;; Note: The group columns are the first columns in the returned dataset.
         (-> (tc/dataset [["two" "a" 2020 0 nil nil]
                          ["two" "a" 2021 1   0   1]
                          ["two" "a" 2022 0   1  -1]]
                         {:column-names [:group-col-1 :group-col-2 :calendar-year :count :count-previous :count-diff]}))))
    ;; BUT note that the values returned are taken from the first row in the input dataset:
    (is (=
         ;; Note: `tc/group-by` with the same `group-cols` will ensure all have the same values,
         ;;        but to illustrate, consider the following, noting that the dataset is not sorted by `:calendar-year`:
         (-> (tc/dataset [[2021 1 "two"  ]
                          [2020 0 "one"  ]
                          [2022 0 "three"]]
                         {:column-names [:calendar-year :count :group]})
             (asd/add-previous-year-diff-to-sparse {:group-cols :group}))
         ;; Note: The `:group` column is filled with the value of "two",
         ;;       taken from the first record (not the record with smallest `:calendar-year`).
         (-> (tc/dataset [["two" 2020 0 nil nil]
                          ["two" 2021 1   0   1]
                          ["two" 2022 0   1  -1]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]}))))
    ;; …which is how they get filled in for `:calendar-year`s omitted from sparse input:
    (is (=
         ;; Note: Input only has 2021, but range specified as 2020--2022.
         (-> (tc/dataset [[2021 1 "two"]]
                         {:column-names [:calendar-year :count :group]})
             (asd/add-previous-year-diff-to-sparse {:min-cy     2020
                                                    :max-cy     2022
                                                    :group-cols :group}))
         (-> (tc/dataset [["two" 2020 0 nil nil]
                          ["two" 2021 1   0   1]
                          ["two" 2022 0   1  -1]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]}))))
    ;; BUT any other columns are not carried through to the output dataset:
    (is (=
         ;; Note: Extra column `:extra-col` with value "(two)".
         (-> (tc/dataset [[2021 1 "two" "(two)"]]
                         {:column-names [:calendar-year :count :group :extra-col]})
             (asd/add-previous-year-diff-to-sparse {:min-cy     2020
                                                    :max-cy     2022
                                                    :group-cols :group}))
         ;; Note: No `:extra-col` in the returned dataset.
         (-> (tc/dataset [["two" 2020 0 nil nil]
                          ["two" 2021 1   0   1]
                          ["two" 2022 0   1  -1]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]})))))

  (testing "Non-unique keys"
    ;; `:calendar-year` must be unique: If it isn't then you get an error:
    (is (= (try
             (-> (tc/dataset [[2021 1]
                              [2022 2]
                              [2022 3]]
                             {:column-names [:calendar-year :count]})
                 asd/add-previous-year-diff-to-sparse)
             (catch Exception e (str "Caught exception: " (.getMessage e))))
           "Caught exception: Some `:calendar-year`s repeated in input dataset.")))

  (testing "Different column-names"
    ;; Can specify `value-col`, `previous-value-col` & `diff-col`:
    (is (=
         ;; Note: Value col is `:ehcps`, and specified custom `previous-value-col` & `diff-col`.
         (-> (tc/dataset [[2021 1]]
                         {:column-names [:calendar-year :ehcps]})
             (asd/add-previous-year-diff-to-sparse {:min-cy             2020
                                                    :max-cy             2022
                                                    :value-col          :ehcps
                                                    :previous-value-col :last-year-ehcps
                                                    :diff-col           :yoy-ehcp-change}))
         ;; Note:
         (-> (tc/dataset [[2020 0 nil nil]
                          [2021 1   0   1]
                          [2022 0   1  -1]]
                         {:column-names [:calendar-year :ehcps :last-year-ehcps :yoy-ehcp-change]}))))))

(deftest add-previous-year-diff-to-sparse-by-group
  (testing "Grouping"
    ;; Complete dataset:
    (is (=
         ;; Note: `:calendar-year`s 2021--2022 for both `:group`s "one" and "two", with `:group` specified as `group-cols`.
         (-> (tc/dataset [["one" 2021 1]
                          ["one" 2022 0]
                          ["one" 2023 0]
                          ["one" 2024 2]
                          ["two" 2021 0]
                          ["two" 2022 3]
                          ["two" 2023 4]
                          ["two" 2024 0]]
                         {:column-names [:group :calendar-year :count]})
             (asd/add-previous-year-diff-to-sparse-by-group {:group-cols :group}))
         ;; Note:
         ;; - Lagging is within group.
         (-> (tc/dataset [["one" 2021 1 nil nil]
                          ["one" 2022 0   1  -1]
                          ["one" 2023 0   0   0]
                          ["one" 2024 2   0   2]
                          ["two" 2021 0 nil nil]
                          ["two" 2022 3   0   3]
                          ["two" 2023 4   3   1]
                          ["two" 2024 0   4  -4]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]}))))
    ;; Sparse version
    (is (=
         ;; Note: In this example `:calendar-year` is unique across the whole dataset, not just within `:group`s.
         (-> (tc/dataset [["one" 2021 1]
                          ["one" 2024 2]
                          ["two" 2022 3]
                          ["two" 2023 4]]
                         {:column-names [:group :calendar-year :count]})
             (asd/add-previous-year-diff-to-sparse-by-group {:group-cols :group}))
         ;; => _unnamed [8 5]:
         ;;    | :group | :calendar-year | :count | :count-previous | :count-diff |
         ;;    |--------|---------------:|-------:|----------------:|------------:|
         ;;    |    one |           2021 |      1 |                 |             |
         ;;    |    one |           2022 |      0 |               1 |          -1 |
         ;;    |    one |           2023 |      0 |               0 |           0 |
         ;;    |    one |           2024 |      2 |               0 |           2 |
         ;;    |    two |           2021 |      0 |                 |             |
         ;;    |    two |           2022 |      3 |               0 |           3 |
         ;;    |    two |           2023 |      4 |               3 |           1 |
         ;;    |    two |           2024 |      0 |               4 |          -4 |
         ;; Note:
         ;; - Although `:calendar-year` is itself a unique key for the input dataset, lagging is done within group,
         ;;   so the group "two" 2023 record (with value 4) is not the previous record for the group "one" 2024 record.
         ;; - The range for `:calendar-year`:
         ;;   - In the absence of specification via `:min-index` & `:max-index`,
         ;;     is taken across the entire input dataset.
         ;;   - Here this yields min-index of 2021 and max-index of 2024 (both from group "one").
         ;;   - The range is applied to each group.
         (-> (tc/dataset [["one" 2021 1 nil nil]
                          ["one" 2022 0   1  -1]
                          ["one" 2023 0   0   0]
                          ["one" 2024 2   0   2]
                          ["two" 2021 0 nil nil]
                          ["two" 2022 3   0   3]
                          ["two" 2023 4   3   1]
                          ["two" 2024 0   4  -4]]
                         {:column-names [:group :calendar-year :count :count-previous :count-diff]}))))))

(comment
  (clojure.test/run-tests)

  )
