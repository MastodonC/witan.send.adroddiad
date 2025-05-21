(ns witan.send.adroddiad.tablecloth-utils-test
  "Testing for library of tablecloth utilities."
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.tablecloth-utils :as tc-utils]
            [tablecloth.api :as tc]))



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

(comment ;; Run tests
  (clojure.test/run-tests)
  
  :rcf)
