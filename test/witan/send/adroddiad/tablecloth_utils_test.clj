(ns witan.send.adroddiad.tablecloth-utils-test
  "Testing for library of tablecloth utilities."
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.tablecloth-utils :as tc-utils]
            [tablecloth.api :as tc]))



;;; # Non-unique
(deftest non-unique-by
  (testing "With no arguments returns non-unique rows of dataset."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             tc-utils/non-unique-by)
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}]))))

  (testing "Similarly with `columns-selector` `:all`."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by :all))
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}]))))

  (testing "With `columns-selector` specifying a single column, returns rows with non-unique values in that column."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by :a))
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 2}
                      {:a 1, :b 2, :c 1}]))))

  (testing "With `columns-selector` specifying a multiple columns, returns rows with non-unique values for those columns."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by [:a :b]))
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 2}]))))

  (testing "Works within groups of a grouped dataset (`groups->seq example`)."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by [:b])
             tc/groups->seq)
         ;; Note: Using `tc/groups->seq` to turn grouped dataset into a seq of datasets.
         (sequence [(tc/dataset [{:a 1, :b 1, :c 1}
                                 {:a 1, :b 1, :c 1}
                                 {:a 1, :b 1, :c 2}])
                    (tc/dataset nil {:column-names [:a :b :c]})]))))

  (testing "Works within groups of a grouped dataset (`tc/ungroup` example)."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by [:b])
             tc/ungroup)
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 2}]))))

  (testing "Specification of `num-rows-same-colname` returns count of matching rows in that column."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by [:a :b] :num-rows-same-colname :row-count))
         (tc/dataset [{:a 1, :b 1, :c 1, :row-count 3}
                      {:a 1, :b 1, :c 1, :row-count 3}
                      {:a 1, :b 1, :c 2, :row-count 3}]))))

  (testing (str "If `num-rows-same-colname` is already in the dataset "
                "then it will be overwritten!")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by [:a :b] :num-rows-same-colname :b))
         (tc/dataset [{:a 1, :b 3, :c 1}
                      {:a 1, :b 3, :c 1}
                      {:a 1, :b 3, :c 2}]))))

  (testing (str "If `num-rows-same-colname` is not specified, "
                "then `:__num-rows-same` is used internally, "
                "such that if the dataset has a column so named it will be dropped")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1, :__num-rows-same "*"}
                          {:a 1, :b 1, :c 1, :__num-rows-same "*"}
                          {:a 1, :b 1, :c 2, :__num-rows-same "*"}
                          {:a 1, :b 2, :c 1, :__num-rows-same "*"}
                          {:a 2, :b 1, :c 1, :__num-rows-same "*"}])
             (tc-utils/non-unique-by [:a :b]))
         (tc/dataset [{:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 1}
                      {:a 1, :b 1, :c 2}]))))

  (testing "Replaces deprecated `select-non-unique-rows` function."
    (is (let [ds (tc/dataset [{:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 2}
                              {:a 1, :b 2, :c 1}
                              {:a 2, :b 1, :c 1}])]
          (= (-> ds tc-utils/select-non-unique-rows)
             (-> ds tc-utils/non-unique-by)))))

  (testing "Replaces deprecated `select-non-unique-key-rows` function."
    (is (let [ds (tc/dataset [{:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 2}
                              {:a 1, :b 2, :c 1}
                              {:a 2, :b 1, :c 1}])]
          (= (-> ds (tc-utils/select-non-unique-key-rows [:a :b]))
             (-> ds (tc-utils/non-unique-by [:a :b])))))))


(deftest non-unique-by-keys
  (testing (str "With no arguments returns a dataset containing one example of "
                "each row that appears multiple times in the dataset.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             tc-utils/non-unique-by-keys)
         (tc/dataset [{:a 1, :b 1, :c 1}]))))

  (testing "Similarly with `columns-selector` `:all`."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by-keys :all))
         (tc/dataset [{:a 1, :b 1, :c 1}]))))

  (testing (str "Which is essentially the same as"
                "`tc-utils/non-unique-by` followed by `tc/unique-by` "
                "(but simpler).")
    (is (let [ds (tc/dataset [{:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 2}
                              {:a 1, :b 2, :c 1}
                              {:a 2, :b 1, :c 1}])]
          (= (-> ds tc-utils/non-unique-by-keys)
             (-> ds tc-utils/non-unique-by tc/unique-by)))))

  (testing (str "With a subset of columns specified, "
                "returns a dataset with the combinations of these columns that "
                "appear more than once in the dataset: I.e. the non-unique keys.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by-keys [:a :b]))
         (tc/dataset [{:a 1, :b 1}]))))

  (testing (str "Which is essentially the same as"
                "`tc-utils/non-unique-by` "
                "followed by `tc/select-columns` for the key columns "
                "followed by `tc/unique-by` "
                "(but simpler).")
    (is (let [ds (tc/dataset [{:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 2}
                              {:a 1, :b 2, :c 1}
                              {:a 2, :b 1, :c 1}])]
          (= (-> ds (tc-utils/non-unique-by-keys [:a :b]))
             (-> ds
                 (tc-utils/non-unique-by [:a :b])
                 (tc/select-columns [:a :b])
                 tc/unique-by)))))
  
  (testing (str "Supports specification of columns via supported tablecloth " 
                "`columns-selector` mechanisms, for example regex.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by-keys #"^:[ab]$"))
         (tc/dataset [{:a 1, :b 1}]))))
  
  (testing "Works within groups of a grouped dataset, but loses the grouping columns (`groups->seq example`)."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:b])
             tc/groups->seq)
         ;; Note: 
         ;; - Using `tc/groups->seq` to turn grouped dataset into a seq of datasets.
         ;; - Datasets do not contain the grouping columns `:a`: possibly a tablecloth issue?
         (sequence [(tc/dataset [{:b 1}])
                    (tc/dataset nil {:column-names [:b]})]))))
  
  (testing "Works within groups of a grouped dataset, but loses the grouping columns (`tc/ungroup` example)."
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:b])
             tc/ungroup)
         ;; Note: Returned datasets does not contain the grouping column `:a`: possibly a tablecloth issue?
         (tc/dataset [{:b 1}]))))
  
  (comment ;; Possible issue with `tc/ungroup` not putting grouping columns back on?
    (-> (tc/dataset {:a 1 :b 1})
        (tc/group-by [:a])
        (tc/process-group-data #(tc/drop-columns % [:a]))
        tc/ungroup)
    ;;=> _unnamed [1 1]:
    ;;   
    ;;   | :b |
    ;;   |---:|
    ;;   |  1 |
    ;;   
    :rcf)
  
  (testing (str "For grouped data, can effect a workaround retaining the " 
                "grouping columns by using: "
                "`tc-utils/non-unique-by` "
                "followed by `tc/select-columns` for the groung and key columns "
                "followed by `tc/unique-by`.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by [:b])
             tc/ungroup
             (tc/select-columns [:a :b])
             tc/unique-by)
         (tc/dataset [{:a 1, :b 1}]))))

  (testing (str "Returns dataset with count of number of matching rows in column "
                "`num-rows-same-colname` if specified")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by-keys [:a :b] :num-rows-same-colname :num-rows))
         (tc/dataset [{:a 1, :b 1, :num-rows 3}]))))

  (testing "Replaces deprecated `select-non-unique-keys` function."
    (is (let [ds (tc/dataset [{:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 1}
                              {:a 1, :b 1, :c 2}
                              {:a 1, :b 2, :c 1}
                              {:a 2, :b 1, :c 1}])]
          (= (-> ds (tc-utils/select-non-unique-keys [:a :b]))
             (-> ds (tc-utils/non-unique-by-keys [:a :b] :num-rows-same-colname :num-rows)))))))



;;; # dataset <-> map conversion
(deftest ds->hash-map
  (testing "For two column dataset using defaults: 1st col selected for keys, second as vals."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]})
             tc-utils/ds->hash-map)
         {"c1r1" "c2r1"
          "c1r2" "c2r2"})))

  (testing "Returned map is a clojure.lang.PersistentArrayMap."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]})
             tc-utils/ds->hash-map
             type
             str)
         "class clojure.lang.PersistentArrayMap")))

  (testing "For three column dataset using defaults: 1st col selected for keys, remaining (as row maps) as vals."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]})
             tc-utils/ds->hash-map)
         {"c1r1" {:col-2 "c2r1"
                  :col-3 "c3r1"}
          "c1r2" {:col-2 "c2r2"
                  :col-3 "c3r2"}})))

  (testing "Specify a single `key-cols`: That col selected for keys, remaining (as row maps) as vals."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map :key-cols :col-2))
         {"c2r1" {:col-1 "c1r1"
                  :col-3 "c3r1"
                  :col-4 "c4r1"}
          "c2r2" {:col-1 "c1r2"
                  :col-3 "c3r2"
                  :col-4 "c4r2"}})))

  (testing "Specify subset of remaining columns as `val-col`s."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map {:key-cols :col-2
                                     :val-cols [:col-3 :col-4]}))
         {"c2r1" {:col-3 "c3r1"
                  :col-4 "c4r1"}
          "c2r2" {:col-3 "c3r2"
                  :col-4 "c4r2"}})))

  (testing "Specify `val-col`s using a regex (passed to `tc/column-names.`)"
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map {:key-cols :col-2
                                     :val-cols #"^:col-[34]$"}))
         {"c2r1" {:col-3 "c3r1"
                  :col-4 "c4r1"}
          "c2r2" {:col-3 "c3r2"
                  :col-4 "c4r2"}})))

  (testing "Ask for vals from single `val-cols` to be returned as row maps."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map {:key-cols              :col-2
                                     :val-cols              [:col-3]
                                     :single-val-col-as-map true}))
         {"c2r1" {:col-3 "c3r1"}
          "c2r2" {:col-3 "c3r2"}})))

  (testing "Ask for keys from a single `key-cols` to be returned as row maps."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map {:key-cols              :col-2
                                     :val-cols              [:col-3]
                                     :single-key-col-as-map true
                                     :single-val-col-as-map true}))
         {{:col-2 "c2r1"} {:col-3 "c3r1"}
          {:col-2 "c2r2"} {:col-3 "c3r2"}})))

  (testing "Include key columns in the value maps."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]})
             (tc-utils/ds->hash-map {:key-cols :col-2
                                     :val-cols [:col-2 :col-3]}))
         {"c2r1" {:col-2 "c2r1"
                  :col-3 "c3r1"}
          "c2r2" {:col-2 "c2r2"
                  :col-3 "c3r2"}}))))


(deftest ds->sorted-map-by
  (testing "Returns a map sorted by dataset row order by default."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]})
             tc-utils/ds->sorted-map-by)
         {"c1r1" "c2r1"
          "c1r2" "c2r2"})))

  (testing "Returned map is a clojure.lang.PersistentTreeMap."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]})
             tc-utils/ds->sorted-map-by
             type
             str)
         "class clojure.lang.PersistentTreeMap")))

  (testing "Specify an order column (which by default is included in the vals maps)."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :order [1      2]})
             (tc-utils/ds->sorted-map-by :order-col :order))
         {"c1r1" {:col-2 "c2r1"
                  :order 1}
          "c1r2" {:col-2 "c2r2"
                  :order 2}})))

  (testing "With an ordering other than the dataset order."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :order [2      1]})
             (tc-utils/ds->sorted-map-by :order-col :order))
         {"c1r2" {:col-2 "c2r2"
                  :order 1}
          "c1r1" {:col-2 "c2r1"
                  :order 2}})))

  (testing "Other trailing keyword-value arguments pass through to `ds->hash-map`."
    (is (=
         (-> (tc/dataset {:col-1 ["c1r1" "c1r2"]
                          :col-2 ["c2r1" "c2r2"]
                          :col-3 ["c3r1" "c3r2"]
                          :col-4 ["c4r1" "c4r2"]
                          :order [2      1]})
             (tc-utils/ds->sorted-map-by {:key-cols              :col-2
                                          :val-cols              [:col-3]
                                          :single-key-col-as-map false
                                          :single-val-col-as-map true
                                          :order-col             :order}))
         {"c2r2" {:col-3 "c3r2"}
          "c2r1" {:col-3 "c3r1"}}))))


(deftest map->ds
  (testing "Basic usage for a map where keys and vals are not maps, with default column-names."
    (is (=
         (-> {"k-r1" "v-r1"
              "k-r2" "v-r2"}
             tc-utils/map->ds)
         (tc/dataset {:keys ["k-r1" "k-r2"]
                      :vals ["v-r1" "v-r2"]}))))

  (testing "Specify column-names"
    (is (=
         (-> {"k-r1" "v-r1"
              "k-r2" "v-r2"}
             (tc-utils/map->ds {:keys-col-name :k, :vals-col-name :v}))
         (tc/dataset {:k ["k-r1" "k-r2"]
                      :v ["v-r1" "v-r2"]}))))

  (testing "From map with vals that are maps."
    (is (=
         (-> {"k-r1" {:v1 "v1r1", :v2 "v2r1"}
              "k-r2" {:v1 "v1r2", :v2 "v2r2"}}
             (tc-utils/map->ds :keys-col-name :k))
         (tc/dataset {:k  ["k-r1" "k-r2"]
                      :v1 ["v1r1" "v1r2"]
                      :v2 ["v2r1" "v2r2"]}))))

  (testing "From map with keys & vals that are both maps."
    (is (=
         (-> {{:k1 "k1r1" :k2 "k2r1"} {:v1 "v1r1", :v2 "v2r1"}
              {:k1 "k1r2" :k2 "k2r2"} {:v1 "v1r2", :v2 "v2r2"}}
             tc-utils/map->ds)
         (tc/dataset {:k1 ["k1r1" "k1r2"]
                      :k2 ["k2r1" "k2r2"]
                      :v1 ["v1r1" "v1r2"]
                      :v2 ["v2r1" "v2r2"]}))))

  (testing "From map with vals that are maps but with inconsistent keys."
    (is (=
         (-> (-> {"k-r1" {:v1 "v1r1", :v2 "v2r1"}
                  "k-r2" {:v1 "v1r2", :v3 "v3r2"}}
                 (tc-utils/map->ds :keys-col-name :k)))
         (tc/dataset {:k  ["k-r1" "k-r2"]
                      :v1 ["v1r1" "v1r2"]
                      :v2 ["v2r1"  nil]
                      :v3 [nil    "v3r2"]}))))

  (testing "From map with vals a mix of values and maps."
    (is (=
         (-> (-> {"k-r1" {:v1 "v1r1", :v2 "v2r1"}
                  "k-r2" "v1r2"}
                 (tc-utils/map->ds :keys-col-name :k)))
         (tc/dataset {:k    ["k-r1" "k-r2"]
                      :v1   ["v1r1"  nil]
                      :v2   ["v2r1"  nil]
                      :vals [nil    "v1r2"]}))))

  (testing "…but specifying `:vals-col-name` to match one of the vals map keys."
    (is (=
         (-> (-> {"k-r1" {:v1 "v1r1", :v2 "v2r1"}
                  "k-r2" "v1r2"}
                 (tc-utils/map->ds {:keys-col-name :k
                                    :vals-col-name :v1})))
         (tc/dataset {:k    ["k-r1" "k-r2"]
                      :v1   ["v1r1" "v1r2"]
                      :v2   ["v2r1"  nil]})))))



;;; # Run tests
(comment ;; Run tests
  (clojure.test/run-tests)
  
  :rcf)
