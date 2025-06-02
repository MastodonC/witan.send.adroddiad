(ns witan.send.adroddiad.school-phase-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.school-phase :as school-phase]))

(deftest ncy->school-phase-map
  (testing "Default mapping of NCY to key stage."
    (is (= (school-phase/ncy->school-phase-map)
           (merge (zipmap [-4 -3 -2 -1]       (repeat "early-childhood"))
                  (zipmap [0 1 2 3 4 5 6]     (repeat "primary"))
                  (zipmap [7 8 9 10 11]       (repeat "secondary"))
                  (zipmap [12 13 14]          (repeat "post-16"))
                  (zipmap [15 16 17 18 19 20] (repeat "post-19"))))))

  (testing "Custom `school-phases` definitions, with `:ncys` as sets."
    (is (= (-> {"Phase A" {:ncys #{109 108}}
                "Phase B" {:ncys #{110 111 112}}}
               school-phase/ncy->school-phase-map)
           ;; Note: Map is sorted by NCY
           (sorted-map 108 "Phase A"
                       109 "Phase A"
                       110 "Phase B"
                       111 "Phase B"
                       112 "Phase B"))))

  (testing "Custom `school-phases` definitions, with `:ncys` as vector and sequence."
    (is (= (-> {"Phase A" {:ncys [109 108]}
                "Phase B" {:ncys '(110 111 112)}}
               school-phase/ncy->school-phase-map)
           ;; Note: Map is sorted by NCY
           (sorted-map 108 "Phase A"
                       109 "Phase A"
                       110 "Phase B"
                       111 "Phase B"
                       112 "Phase B"))))

  (testing "First school-phase defined containing NCY is used when there are overlaps."
    (is (= (-> (sorted-map
                "Phase A" {:ncys [108 109 110]}
                "Phase B" {:ncys [110 111 112]})
               keys)
           ;; Note: Map sorted by keys, so "Phase A" is defined first
           '("Phase A" "Phase B")))
    (is (= (-> (sorted-map
                "Phase A" {:ncys [108 109 110]}
                "Phase B" {:ncys [110 111 112]})
               school-phase/ncy->school-phase-map)
           ;; Note: So school-phase for NCY 110 is "Phase A"
           (sorted-map 108 "Phase A"
                       109 "Phase A"
                       110 "Phase A"
                       111 "Phase B"
                       112 "Phase B")))
    ;; Reversing the definition order
    (is (= (-> (sorted-map-by #(compare %2 %1)
                              "Phase A" {:ncys [108 109 110]}
                              "Phase B" {:ncys [110 111 112]})
               keys)
           ;; Note: Map sorted by keys descending, so "Phase B" is defined first
           '("Phase B" "Phase A")))
    (is (= (-> (sorted-map-by #(compare %2 %1)
                              "Phase A" {:ncys [108 109 110]}
                              "Phase B" {:ncys [110 111 112]})
               school-phase/ncy->school-phase-map)
           ;; Note: So school-phase for NCY 110 is "Phase B"
           (sorted-map 108 "Phase A"
                       109 "Phase A"
                       110 "Phase B"
                       111 "Phase B"
                       112 "Phase B"))))

  (testing "Repetition of NCY within a definition is ignored."
    (is (= (-> {"Phase C" {:ncys [120 120 120]}}
               school-phase/ncy->school-phase-map)
           {120 "Phase C"}))))

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
             (every? #{"post-19"})))))

(comment
  (clojure.test/run-tests))
