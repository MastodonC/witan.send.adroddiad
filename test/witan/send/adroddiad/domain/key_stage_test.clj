(ns witan.send.adroddiad.domain.key-stage-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.domain.key-stage :as key-stage]))

(deftest ncy->key-stage-map
  (testing "Default mapping of NCY to key stage."
    (is (= (key-stage/ncy->key-stage-map)
           (merge (zipmap [-4 -3 -2 -1 0]     (repeat "early-years"))
                  (zipmap [1 2]               (repeat "key-stage-1"))
                  (zipmap [3 4 5 6]           (repeat "key-stage-2"))
                  (zipmap [7 8 9]             (repeat "key-stage-3"))
                  (zipmap [10 11]             (repeat "key-stage-4"))
                  (zipmap [12 13 14]          (repeat "key-stage-5"))
                  (zipmap [15 16 17 18 19 20] (repeat "post-19"))))))

  (testing "Custom `key-stages` definitions, with `:ncys` as sets."
    (is (= (-> {"Stage A" {:ncys #{109 108}}
                "Stage B" {:ncys #{110 111 112}}}
               key-stage/ncy->key-stage-map)
           ;; Note: Map is sorted by NCY
           (sorted-map 108 "Stage A"
                       109 "Stage A"
                       110 "Stage B"
                       111 "Stage B"
                       112 "Stage B"))))
  
  (testing "Custom `key-stages` definitions, with `:ncys` as vector and sequence."
    (is (= (-> {"Stage A" {:ncys [109 108]}
                "Stage B" {:ncys '(110 111 112)}}
               key-stage/ncy->key-stage-map)
           ;; Note: Map is sorted by NCY
           (sorted-map 108 "Stage A"
                       109 "Stage A"
                       110 "Stage B"
                       111 "Stage B"
                       112 "Stage B"))))

  (testing "First key-stage defined containing NCY is used when there are overlaps."
    (is (= (-> (sorted-map
                "Stage A" {:ncys [108 109 110]}
                "Stage B" {:ncys [110 111 112]})
               keys)
           ;; Note: Map sorted by keys, so "Stage A" is defined first
           '("Stage A" "Stage B")))
    (is (= (-> (sorted-map
                "Stage A" {:ncys [108 109 110]}
                "Stage B" {:ncys [110 111 112]})
               key-stage/ncy->key-stage-map)
           ;; Note: So key-stage for NCY 110 is "Stage A"
           (sorted-map 108 "Stage A"
                       109 "Stage A"
                       110 "Stage A"
                       111 "Stage B"
                       112 "Stage B")))
    ;; Reversing the definition order
    (is (= (-> (sorted-map-by #(compare %2 %1)
                              "Stage A" {:ncys [108 109 110]}
                              "Stage B" {:ncys [110 111 112]})
               keys)
           ;; Note: Map sorted by keys descending, so "Stage B" is defined first
           '("Stage B" "Stage A")))
    (is (= (-> (sorted-map-by #(compare %2 %1)
                              "Stage A" {:ncys [108 109 110]}
                              "Stage B" {:ncys [110 111 112]})
               key-stage/ncy->key-stage-map)
           ;; Note: So key-stage for NCY 110 is "Stage B"
           (sorted-map 108 "Stage A"
                       109 "Stage A"
                       110 "Stage B"
                       111 "Stage B"
                       112 "Stage B"))))

  (testing "Repetition of NCY within a definition is ignored."
    (is (= (-> {"Stage C" {:ncys [120 120 120]}}
               key-stage/ncy->key-stage-map)
           {120 "Stage C"}))))

(deftest ncy->key-stage
  (testing "For NCY not in (range -4 (inc 20)), key-stage should be nil."
    (is (->> nil key-stage/ncy->key-stage nil?))
    (is (->>  -5 key-stage/ncy->key-stage nil?))
    (is (->>  21 key-stage/ncy->key-stage nil?)))

  (testing "NCYs -4 to 0 are early-years."
    (is (->> [-4 -3 -2 -1  0]
             (map key-stage/ncy->key-stage)
             (every? #{"early-years"}))))

  (testing "NCYs 1 & 2 are key-stage-1."
    (is (->> [1 2]
             (map key-stage/ncy->key-stage)
             (every? #{"key-stage-1"}))))

  (testing "NCYs 3 to 6 are key-stage-2."
    (is (->> [3 4 5 6]
             (map key-stage/ncy->key-stage)
             (every? #{"key-stage-2"}))))

  (testing "NCYs 7 to 9 are key-stage-3."
    (is (->> [7 8 9]
             (map key-stage/ncy->key-stage)
             (every? #{"key-stage-3"}))))

  (testing "NCYs 10 & 11 are key-stage-4."
    (is (->> [10 11]
             (map key-stage/ncy->key-stage)
             (every? #{"key-stage-4"}))))

  (testing "NCYs 12 to 14 are key-stage-5."
    (is (->> [12 13 14]
             (map key-stage/ncy->key-stage)
             (every? #{"key-stage-5"}))))

  (testing "NCYs 15 to 20 are beyond the national curriculum and we label as post-19."
    (is (->> [15 16 17 18 19 20]
             (map key-stage/ncy->key-stage)
             (every? #{"post-19"})))))

(comment ;; Run tests
  (clojure.test/run-tests)
  
  :rcf)
