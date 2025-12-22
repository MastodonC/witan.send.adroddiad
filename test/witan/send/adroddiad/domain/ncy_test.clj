(ns witan.send.adroddiad.domain.ncy-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.send.adroddiad.domain.ncy :as ncy]
            [tablecloth.api :as tc])
  (:import [java.time LocalDate]))

(deftest age-at-start-of-school-year
  (testing "A child born on 01-Sep-2016 would be 3 at the start of the 2020/21 school year."
    (is (=  3 (ncy/age-at-start-of-school-year-for-census-year 2021
                                                               2016 9)))
    (is (=  3 (ncy/age-at-start-of-school-year-for-census-year 2021
                                                               (LocalDate/of 2016  9  1))))
    (is (=  3 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2020  9  1)
                                                        (LocalDate/of 2016  9  1))))
    (is (=  3 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2021  1 14)
                                                        (LocalDate/of 2016  9  1))))
    (is (=  3 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2021  8 31)
                                                        (LocalDate/of 2016  9  1)))))

  (testing "…but a child born on 31-Aug-2016 would be 4 at the start of the 2020/21 school year."
    (is (=  4 (ncy/age-at-start-of-school-year-for-census-year 2021
                                                               2016 8)))
    (is (=  4 (ncy/age-at-start-of-school-year-for-census-year 2021
                                                               (LocalDate/of 2016  8 31))))
    (is (=  4 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2020 9  1)
                                                        (LocalDate/of 2016 8 31)))))

  (testing "A child born on 31-Aug-2016 would be 0 at the start of the 2016/17 school year."
    (is (=  0 (ncy/age-at-start-of-school-year-for-census-year 2017
                                                               2016 8)))
    (is (=  0 (ncy/age-at-start-of-school-year-for-census-year 2017
                                                               (LocalDate/of 2016  8 31))))
    (is (=  0 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2016  9  1)
                                                        (LocalDate/of 2016  8 31)))))

  (testing "Note that the algorithm will return -ve ages."
    (is (= -1 (ncy/age-at-start-of-school-year-for-census-year 2017
                                                               2016 9)))
    (is (= -1 (ncy/age-at-start-of-school-year-for-census-year 2017
                                                               (LocalDate/of 2016  9  1))))
    (is (= -1 (ncy/age-at-start-of-school-year-for-date (LocalDate/of 2016  9  1)
                                                        (LocalDate/of 2016  9  1))))))

(deftest ncy<->age-at-start-of-school-year
  (testing "Age at start of reception should be 4 years, per gov.uk/schools-admissions/school-starting-age."
    (is (= 4 (ncy/ncy->age-at-start-of-school-year 0))))

  (testing "Age at start of NCY -4 to 20 should be 0 to 24 respectively."
    (is (= (->> (range -4 (inc 20))
                (map ncy/ncy->age-at-start-of-school-year))
           (range 0  (inc 24)))))

  (testing "ncy->age… should return nil for non-integer NCYs outside -4 to 20 inclusive."
    (is (= (->> [-5 0.5 21]
                (map ncy/ncy->age-at-start-of-school-year))
           '(nil nil nil))))

  (testing "NCY entered at age 4 should be 0 (reception), per gov.uk/schools-admissions/school-starting-age."
    (is (= 0 (ncy/age-at-start-of-school-year->ncy 4))))

  (testing "NCY entered at age 0 to 24 should be -4 to 20 respectively."
    (is (= (->> (range  0 (inc 24))
                (map ncy/age-at-start-of-school-year->ncy))
           (range -4 (inc 20)))))

  (testing "Round trip test: ncy->age… followed by age…->ncy should return the same for integers -4 to 20 inclusive."
    (is (= (->> (range -4 (inc 20))
                (map ncy/ncy->age-at-start-of-school-year)
                (map ncy/age-at-start-of-school-year->ncy))
           (range -4 (inc 20)))))

  (testing "age…->ncy should return nil for non-integer age outside 0 to 24 inclusive."
    (is (= (->> [-1 0.5 25]
                (map ncy/age-at-start-of-school-year->ncy))
           '(nil nil nil)))))

(def test-impute-01-ds
  (-> (tc/concat (tc/dataset {:id  1 :calendar-year [2000 2001 2002] :academic-year [nil]})
                 (tc/dataset {:id  2 :calendar-year [2000 2001 2002] :academic-year [nil]}))
      (tc/add-column :row-id (range))))

(def test-impute-02-ds
  (-> (tc/concat (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [0   1   2]})
                 (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [0 nil nil]})
                 (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [nil   1 nil]})
                 (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [nil nil   2]})
                 (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [0   1 nil]})
                 (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [nil   1   2]})
                 (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [nil  -4 nil]})
                 (tc/dataset {:id 10 :calendar-year [2000 2001 2002] :academic-year [nil  20 nil]}))
      (tc/add-column :row-id (range))
      (tc/reorder-columns [:row-id])))

(def test-impute-02-rows
  (into (sorted-set) ((tc/drop-missing test-impute-02-ds [:academic-year]) :row-id)))

(comment ;; EDA: test datasets
  test-impute-01-ds
  ;;=> _unnamed [6 4]:
  ;;   
  ;;   | :id | :calendar-year | :academic-year | :row-id |
  ;;   |----:|---------------:|----------------|--------:|
  ;;   |   1 |           2000 |                |       0 |
  ;;   |   1 |           2001 |                |       1 |
  ;;   |   1 |           2002 |                |       2 |
  ;;   |   2 |           2000 |                |       3 |
  ;;   |   2 |           2001 |                |       4 |
  ;;   |   2 |           2002 |                |       5 |
  ;;   

  (-> test-impute-02-ds
      (vary-meta assoc :print-index-range 1000))
  ;;=> _unnamed [24 4]:
  ;;   
  ;;   | :row-id | :id | :calendar-year | :academic-year |
  ;;   |--------:|----:|---------------:|---------------:|
  ;;   |       0 |   3 |           2000 |              0 |
  ;;   |       1 |   3 |           2001 |              1 |
  ;;   |       2 |   3 |           2002 |              2 |
  ;;   |       3 |   4 |           2000 |              0 |
  ;;   |       4 |   4 |           2001 |                |
  ;;   |       5 |   4 |           2002 |                |
  ;;   |       6 |   5 |           2000 |                |
  ;;   |       7 |   5 |           2001 |              1 |
  ;;   |       8 |   5 |           2002 |                |
  ;;   |       9 |   6 |           2000 |                |
  ;;   |      10 |   6 |           2001 |                |
  ;;   |      11 |   6 |           2002 |              2 |
  ;;   |      12 |   7 |           2000 |              0 |
  ;;   |      13 |   7 |           2001 |              1 |
  ;;   |      14 |   7 |           2002 |                |
  ;;   |      15 |   8 |           2000 |                |
  ;;   |      16 |   8 |           2001 |              1 |
  ;;   |      17 |   8 |           2002 |              2 |
  ;;   |      18 |   9 |           2000 |                |
  ;;   |      19 |   9 |           2001 |             -4 |
  ;;   |      20 |   9 |           2002 |                |
  ;;   |      21 |  10 |           2000 |                |
  ;;   |      22 |  10 |           2001 |             20 |
  ;;   |      23 |  10 |           2002 |                |
  ;;   

  test-impute-02-rows
  ;;=> #{0 1 2 3 7 11 12 13 16 17 19 22}

  :rcf)

(deftest impute-nil-ncy
  (testing (str "Where all `ncy` are nil (so no reference rows), "
                "should not blow up and should return input dataset "
                "(including any other columns - here :row-id).")
    (is (= (-> test-impute-01-ds
               ncy/impute-nil-ncy)
           test-impute-01-ds)))

  (testing "Rows with non-nil `ncy` should be returned as is, regardless of other rows with nil `ncy`."
    (is (= (-> test-impute-02-ds
               ncy/impute-nil-ncy
               (tc/select-rows #(test-impute-02-rows (:row-id %)))
               (tc/order-by [:row-id]))
           (-> test-impute-02-ds
               (tc/select-rows #(test-impute-02-rows (:row-id %)))))))

  (testing (str "For a CYP with a reference row (with non-nil `ncy`), "
                "should impute `ncy` for any rows with nil `ncy`, "
                "otherwise leave as is.")
    (is (= (-> (tc/concat (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [  0 nil nil]})
                          (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [nil   1 nil]})
                          (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [nil nil   2]})
                          (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [  0   1 nil]})
                          (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [nil   1   2]}))
               ncy/impute-nil-ncy)
           (-> (tc/concat (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                          (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                          (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                          (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                          (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]}))))))

  (testing "Note that `ncy` may be imputed outside the SEND range of -4 to 20."
    (is (= (-> (tc/concat (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [nil  -4 nil]})
                          (tc/dataset {:id 10 :calendar-year [2000 2001 2002] :academic-year [nil  20 nil]}))
               ncy/impute-nil-ncy)
           (-> (tc/concat (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [ -5  -4  -3]})
                          (tc/dataset {:id 10 :calendar-year [2000 2001 2002] :academic-year [ 19  20  21]}))))))

  (testing (str "Where imputation occurs (and `ncy-imputed-from` is requested) "
                "the imputation reference census/calendar-year & NCY should "
                "be described in the specified column.")
    (is (= (-> (tc/dataset {:id 11 :calendar-year [2000 2001] :academic-year [nil   1]})
               (ncy/impute-nil-ncy :ncy-imputed-from :ncy-imputed-from))
           (-> (tc/dataset {:id 11 :calendar-year [2000 2001] :academic-year [  0   1]
                            :ncy-imputed-from [{:calendar-year 2001 :academic-year 1} nil]})))))
  
  (testing (str "Where column names are specified they should be used, " 
                "including specification of a separate column for the NCY after imputation.")
    (is (= (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]})
               (ncy/impute-nil-ncy {:cy :cy, :ncy :ncy,
                                    :ncy-imputed :ncyi, :ncy-imputed-from :ncyi-ref, :ref-cy :from-cy, :ref-ncy :from-ncy}))
           (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-ref [{:from-cy 2001 :from-ncy 1} nil]})))))
  
  (testing "Should also be able to specify options as keyword parameters (rather than a map)."
    (is (= (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]})
               (ncy/impute-nil-ncy :cy :cy, :ncy :ncy,
                                   :ncy-imputed :ncyi, :ncy-imputed-from :ncyi-ref, :ref-cy :from-cy, :ref-ncy :from-ncy))
           (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-ref [{:from-cy 2001 :from-ncy 1} nil]})))))
  
  (testing "The algorithm should work with non-keyword column & key names."
    (is (= (-> (tc/dataset {'id 13 "SEN2 census year" [2000 2001] 'NCY [nil   1]})
               (ncy/impute-nil-ncy :id 'id, :cy "SEN2 census year", :ncy 'NCY,
                               :ncy-imputed 'NCY, :ncy-imputed-from 'ncy-imputation-desc, :ref-cy 'from-cy, :ref-ncy "from NCY"))
           (-> (tc/dataset {'id 13 "SEN2 census year" [2000 2001] 'NCY [  0   1] 'ncy-imputation-desc [{'from-cy 2001 "from NCY" 1} nil]})))))
  
  (testing (str "With multiple possible ref. records, " 
                "the one with lowest census/calendar-year should be used (:id 14 & 15)." 
                "If there are multiple such records, " 
                "then the one with smallest NCY is chosen (:id 16)." 
                "Note that if pupils do not progress at a rate of one NCY per " 
                "census/calendar-year then this can result in discontinuities " 
                "if a CYP repeats or goes back a NCY and has missing NCY in " 
                "subsequent years (:id 17 & 18).")
    (is (= (-> (tc/concat (tc/dataset {:id 14 :cy [2000 2001      2002] :ncy [nil   1       2]})
                          (tc/dataset {:id 15 :cy [2000 2001      2002] :ncy [nil   1       1]})
                          (tc/dataset {:id 16 :cy [2000 2001 2001     ] :ncy [nil   1   0    ]})
                          (tc/dataset {:id 17 :cy [2000 2001      2002] :ncy [  1   1     nil]})
                          (tc/dataset {:id 18 :cy [2000 2001      2002] :ncy [  1   0     nil]}))
               (ncy/impute-nil-ncy :cy :cy, :ncy :ncy, :ncy-imputed :ncy, :ncy-imputed-from :ncy-imputed-from))
           (-> (tc/concat (tc/dataset {:id 14 :cy [2000 2001      2002] :ncy [  0   1       2] :ncy-imputed-from [{:cy 2001 :ncy 1} nil nil]})
                          (tc/dataset {:id 15 :cy [2000 2001      2002] :ncy [  0   1       1] :ncy-imputed-from [{:cy 2001 :ncy 1} nil nil]})
                          (tc/dataset {:id 16 :cy [2000 2001 2001     ] :ncy [ -1   1   0    ] :ncy-imputed-from [{:cy 2001 :ncy 0} nil nil]})
                          (tc/dataset {:id 17 :cy [2000 2001      2002] :ncy [  1   1       3] :ncy-imputed-from [nil nil {:cy 2000 :ncy 1}]})
                          (tc/dataset {:id 18 :cy [2000 2001      2002] :ncy [  1   0       3] :ncy-imputed-from [nil nil {:cy 2000 :ncy 1}]})))))))

