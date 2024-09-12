(ns witan.send.adroddiad.school-phase-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.school-phase :as school-phase]))

(deftest ncy->school-phase
  (testing "For NCY not in (range -4 (inc 20)), school-phase should be nil."
    (is (->> nil school-phase/ncy->school-phase nil?))
    (is (->>  -5 school-phase/ncy->school-phase nil?))
    (is (->>  21 school-phase/ncy->school-phase nil?)))

  (testing "For NCYs -4 to -1 school-phase should be \"early-childhood\"."
    (is (->> [-4 -3 -2 -1]
             (map school-phase/ncy->school-phase)
             (every? #{"early-childhood"}))))

  (testing "For NCYs 0 to 6 school-phase should be \"primary\"."
    (is (->> [0 1 2 3 4 5 6]
             (map school-phase/ncy->school-phase)
             (every? #{"primary"}))))

  (testing "For NCYs 7 to 11 school-phase should be \"secondary\"."
    (is (->> [7 8 9 10 11]
             (map school-phase/ncy->school-phase)
             (every? #{"secondary"}))))

  (testing "For NCYs 12 to 14 school-phase should be \"post-16\"."
    (is (->> [12 13 14]
             (map school-phase/ncy->school-phase)
             (every? #{"post-16"}))))

  (testing "For NCYs 15 to 20 school-phase should be \"post-19\"."
    (is (->> [15 16 17 18 19 20]
             (map school-phase/ncy->school-phase)
             (every? #{"post-19"}))))
  )

(comment
  (clojure.test/run-tests)
  )