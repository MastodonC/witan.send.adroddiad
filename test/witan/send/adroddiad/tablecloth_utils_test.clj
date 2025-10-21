(ns witan.send.adroddiad.tablecloth-utils-test
  "Testing for library of tablecloth utilities."
  (:require [clojure.set]
            [clojure.test :refer [deftest testing is]]
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

  (testing (str "Supports specification of columns via supported tablecloth "
                "`columns-selector` mechanisms, for example regex.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc-utils/non-unique-by #"^:[ab]$"))
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
                "`tc-utils/non-unique-by` followed by `tc/unique-by`.")
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
                "followed by `tc/unique-by`.")
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

  (testing (str "Works within groups of a grouped dataset, "
                "but note that by default any grouping columns not also "
                "included in the `columns-selector` will be dropped.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:b])
             tc/groups->seq)
         (sequence [(tc/dataset [{:b 1}])
                    (tc/dataset nil {:column-names [:b]})]))))

  (testing (str "The grouping columns can be reinstated on ungrouping "
                "using the `:add-group-as-column` options of `tc/ungroup`.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:b])
             (tc/ungroup {:add-group-as-column true}))
         (tc/dataset [{:a 1, :b 1}]))))

  (testing (str "Or the grouping columns can be retained "
                "by including them in the the `columns-selector`.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:a :b])
             tc/groups->seq)
         (sequence [(tc/dataset [{:a 1, :b 1}])
                    (tc/dataset nil {:column-names [:a :b]})]))))

  (testing (str "Or specify `:include-grouping-columns` option truthy "
                "to have `non-unique-by-keys` include the grouping keys in "
                "the `columns-selector` for you.")
    (is (=
         (-> (tc/dataset [{:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 1}
                          {:a 1, :b 1, :c 2}
                          {:a 1, :b 2, :c 1}
                          {:a 2, :b 1, :c 1}])
             (tc/group-by [:a])
             (tc-utils/non-unique-by-keys [:b]
                                          {:include-grouping-columns true})
             tc/groups->seq)
         (sequence [(tc/dataset [{:a 1, :b 1}])
                    (tc/dataset nil {:column-names [:a :b]})]))))

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


(deftest sorted-map-by-key-order

  (testing "Returned map is a clojure.lang.PersistentTreeMap."
    (is (instance? clojure.lang.PersistentTreeMap
                   (tc-utils/sorted-map-by-key-order #{} identity))))

  (testing "Returns map with keys sorted by a `key->order` map."
    (let [m {:c "c", :a "a", :b "b"}]
      (is (= (-> m
                 keys)
             ;; Note: hash-map key order here is as specified
             '(:c :a :b)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order (zipmap [:a :c :b] (range)))
                 keys)
             ;; Note: keys now ordered as requested
             '(:a :c :b)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order (clojure.set/map-invert [:b :c :a]))
                 keys)
             ;; Note: keys now ordered as requested
             '(:b :c :a)))))

  (testing "Returns map with keys sorted by a `key->order` function."
    (let [m {:_3 "3", :_1 "1", :_2 "2"}]
      (is (= (-> m
                 keys)
             ;; Note: hash-map key order here is as specified
             '(:_3 :_1 :_2)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order #(-> % name (subs 1) parse-long -))
                 keys)
             ;; Note: keys now ordered as requested, in descending order of the integer
             '(:_3 :_2 :_1)))))

  (testing "Ordering a map by its (unique) values."
    (let [m {:c2 2, :b1 1, :a3 3}]
      (is (= (-> m
                 keys)
             ;; Note: hash-map key order here is as specified
             '(:c2 :b1 :a3)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order m)
                 keys)
             ;; Note: keys now ordered as requested by values
             '(:b1 :c2 :a3)))))

  (testing "Ordering a map by its (non-unique) values."
    (let [m {:c2 2, :b1 1, :a2 2}]
      (is (= (-> m
                 keys)
             ;; Note: hash-map key order here is as specified
             '(:c2 :b1 :a2)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order m)
                 keys)
             ;; Note: keys now ordered as requested by values with ties broken by keys
             '(:b1 :a2 :c2)))))

  (testing "Using `identity` as `key-fn` gives the same effect as `sorted-map`."
    (let [m {:_3 "3", :_1 "1", :_2 "2"}]
      (is (= (-> m
                 keys)
             ;; Note: hash-map key order here is as specified
             '(:_3 :_1 :_2)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order identity)
                 keys)
             ;; Note: keys now ordered
             '(:_1 :_2 :_3)))
      (is (= (-> m
                 (tc-utils/sorted-map-by-key-order identity))
             ;; Note: keys now ordered
             (into (sorted-map) m))))))


(deftest ds->map

  (testing "Returned map is a clojure.lang.PersistentTreeMap."
    (is (instance? clojure.lang.PersistentTreeMap
                   (-> (tc/dataset) (tc-utils/ds->map)))))

  (testing (str "By default, for a 2 column dataset, "
                "returns map with first column as keys and "
                "second column as values.")
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]})
               tc-utils/ds->map)
           {"c1r1" "c2r1"
            "c1r2" "c2r2"
            "c1r3" "c2r3"})))

  (testing (str "By default, for a 3+ column dataset, "
                "returns map with first column as keys and "
                "remaining columns (as maps) as values.")
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3"]})
               tc-utils/ds->map)
           {"c1r1" {:col-2 "c2r1", :col-3 "c3r1"}
            "c1r2" {:col-2 "c2r2", :col-3 "c3r2"}
            "c1r3" {:col-2 "c2r3", :col-3 "c3r3"}})))

  (testing (str "Specify a single `key-cols`: "
                "That col selected for keys, remaining (as row maps) as vals.")
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3"]})
               (tc-utils/ds->map :key-cols :col-2))
           {"c2r1" {:col-1 "c1r1", :col-3 "c3r1", :col-4 "c4r1"}
            "c2r2" {:col-1 "c1r2", :col-3 "c3r2", :col-4 "c4r2"}
            "c2r3" {:col-1 "c1r3", :col-3 "c3r3", :col-4 "c4r3"}})))

  (testing (str "Specify subset of remaining columns as `val-col`s:"
                "Only those cols are used for vals.")
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3"]})
               (tc-utils/ds->map {:key-cols :col-2
                                  :val-cols [:col-3 :col-4]}))
           {"c2r1" {:col-3 "c3r1", :col-4 "c4r1"}
            "c2r2" {:col-3 "c3r2", :col-4 "c4r2"}
            "c2r3" {:col-3 "c3r3", :col-4 "c4r3"}})))

  (testing "Specify `val-col`s using a regex (passed to `tc/column-names.`)"
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3"]})
               (tc-utils/ds->map {:key-cols :col-2
                                  :val-cols #"^:col-[34]$"}))
           {"c2r1" {:col-3 "c3r1", :col-4 "c4r1"}
            "c2r2" {:col-3 "c3r2", :col-4 "c4r2"}
            "c2r3" {:col-3 "c3r3", :col-4 "c4r3"}})))

  (testing (str "Specify a single column as `val-cols`:"
                "Only this column is used for vals, by default not as a maps.")
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3" "c4r3"]})
               (tc-utils/ds->map {:key-cols              :col-2
                                  :val-cols              :col-3}))
           {"c2r1" "c3r1"
            "c2r2" "c3r2"
            "c2r3" "c3r3"})))

  (testing "Override defaults to have keys/vals from single row/column returned as maps."
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3" "c4r3"]})
               (tc-utils/ds->map {:key-cols              :col-2
                                  :val-cols              :col-3
                                  :single-key-col-as-map true
                                  :single-val-col-as-map true}))
           {{:col-2 "c2r1"} {:col-3 "c3r1"}
            {:col-2 "c2r2"} {:col-3 "c3r2"}
            {:col-2 "c2r3"} {:col-3 "c3r3"}})))

  (testing "Include key columns in the value maps."
    (is (= (-> (tc/dataset {:col-1 ["c1r1" "c1r2" "c1r3"]
                            :col-2 ["c2r1" "c2r2" "c2r3"]
                            :col-3 ["c3r1" "c3r2" "c3r3"]
                            :col-4 ["c4r1" "c4r2" "c4r3"]})
               (tc-utils/ds->map {:key-cols  :col-2
                                  :val-cols [:col-2 :col-3]}))
           {"c2r1" {:col-2 "c2r1", :col-3 "c3r1"}
            "c2r2" {:col-2 "c2r2", :col-3 "c3r2"}
            "c2r3" {:col-2 "c2r3", :col-3 "c3r3"}})))

  (testing "Simple zipmap doesn't necessarily retain row order."
    (let [ks '(:first, :second, :third, :fourth, :fifth, :sixth, :seventh, :eighth, :ninth)
          vs '(+1      +2       +3      +4       +5      +6      +7        +8       +9)
          ds (tc/dataset {:col-1 ks, :col-2 vs})]
      (is (not= (-> (zipmap (:col-1 ds)
                            (:col-2 ds))
                    keys)
                ks))))

  (testing "By default `ds->map` retains row order."
    (let [ks '(:first, :second, :third, :fourth, :fifth, :sixth, :seventh, :eighth, :ninth)
          vs '(+1      +2       +3      +4       +5      +6      +7        +8       +9)
          ds (tc/dataset {:col-1 ks, :col-2 vs})]
      (is (= (-> ds
                 tc-utils/ds->map
                 keys)
             ks))))
  
  (testing "Ordering by a column in the dataset."
    (let [ks '(:first, :second, :third, :fourth, :fifth, :sixth, :seventh, :eighth, :ninth)
          vs '(+1      +2       +3      +4       +5      +6      +7        +8       +9)
          os '(-1      -2       -3      -4       -5      -6      -7        -8       -9)
          ds (tc/dataset {:col-1 ks, :col-2 vs, :col-3 os})]
      (is (= (-> ds
                 (tc-utils/ds->map {:key-cols :col-1
                                    :val-cols :col-2
                                    :order-col :col-3})
                 ((partial take 3)))
             ;; Note: Taking the first three key-value pairs for illustration
             '([:ninth 9] [:eighth 8] [:seventh 7])))
      (is (= (-> ds
                 (tc-utils/ds->map {:key-cols :col-1
                                    :val-cols :col-2
                                    :order-col :col-3})
                 keys)
             ;; Note: `os` is reversing the row order
             (reverse ks)))))
    
  )

;;; # Run tests
(comment ;; Run tests
  (clojure.test/run-tests)
  
  :rcf)
