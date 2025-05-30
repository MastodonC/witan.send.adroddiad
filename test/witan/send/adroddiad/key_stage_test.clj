(ns witan.send.adroddiad.key-stage-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.key-stage :as key-stage]))

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



;;; # Run tests
(comment ;; Run tests
  (clojure.test/run-tests)
  
  :rcf)
