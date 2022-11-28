(ns witan.send.adroddiad.ncy
  "Definitions and functions for handling National Curriculum Year"
  (:require [clojure.set :as set]
            [tablecloth.api :as tc]
            [hyperfiddle.rcf :as rcf]))


;;; # Utility functions
(defn- inclusive-range
  "Returns a lazy seq of nums from `start` (inclusive) to `end` (inclusive), by step 1"
  [start end]
  (range start (inc end)))


;;; # National Curriculum Years and ages
(def ncys
  "Set of national curriculum years (NCY), coded numerically, with
  reception as NCY 0 and earlier NCYs as negative integers."
  (into (sorted-set) (inclusive-range -4 20)))


(def ncy->age-at-start-of-school-year 
  "Maps national curriculum year (NCY) to age in whole number of years
  on 31st August prior to starting the school/academic year.

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  NCYs and ages for reception to NCY 11 (children aged 4 to 15 at the
  start of the school/academic year) are per https://www.gov.uk/national-curriculum.
  Extension to NCYs -4 to -1 (ages 0-3) and NCYs 12 to 20, (ages 16-24)
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic year is +4.

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year. This is per:

  - The [gov.uk website](https://www.gov.uk/schools-admissions/school-starting-age),
  which (as of 23-NOV-2022) states:
  \"Most children start school full-time in the September after their
  fourth birthday. This means they’ll turn 5 during their first school
  year. For example, if your child’s fourth birthday is between 1
  September 2021 and 31 August 2022 they will usually start school in
  September 2022.\".

  - The [2022 SEN2 guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf),
  which states in section 2, part 1, item 1.1 that: \"age breakdown
  refers to age as at 31 August 2021\", implying (since this is for
  the January 2022 SEN2 return) that age for SEN2 breakdowns is as of
  31-AUG prior to starting the school year.

  The maximum age for SEND is 25, but per the relevant legistlation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum NCY in
  this map is 20 (age 24 at start of school/academic year).
  "
  (apply sorted-map (interleave ncys (map #(+ % 4) ncys))))


(def age-at-start-of-school-year->ncy
  "Maps age in whole number of years on 31st August prior to starting
  the school/academic year to the corresponding national curriculum year (NCY).

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  Ages and NCYs for children aged 4 to 15 at the start of school/academic
  year (reception to NCY 11) are per https://www.gov.uk/national-curriculum.
  Extension to ages 0-3 (NCYs -4 to -1) and ages 16-24 (NCYs 12 to 20),
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic year is -4.

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year. This is per:

  - The [gov.uk website](https://www.gov.uk/schools-admissions/school-starting-age),
  which (as of 23-NOV-2022) states:
  \"Most children start school full-time in the September after their
  fourth birthday. This means they’ll turn 5 during their first school
  year. For example, if your child’s fourth birthday is between 1
  September 2021 and 31 August 2022 they will usually start school in
  September 2022.\".

  - The [2022 SEN2 guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf),
  which states in section 2, part 1, item 1.1 that: \"age breakdown
  refers to age as at 31 August 2021\", implying (since this is for
  the January 2022 SEN2 return) that age for SEN2 breakdowns is as of
  31-AUG prior to starting the school year.

  The maximum age for SEND is 25, but per the relevant legistlation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum age in
  this map is 24 (NCY 20).
  "
  (set/map-invert ncy->age-at-start-of-school-year))


;;; # Imputation for missing National Curriculum Year
(defn impute-nil-ncy
  "Impute values for National Curriculum Year (NCY) where nil in `ds`.

   Imputation is performed for rows with a nil NCY for pupils with at
   least one other row with a non-nil NCY and non-nil census/calendar
   year, using the first such pair (with smallest census/calendar year)
   as a reference point from which to calculate the nil NCYs, assuming
   the CYP progresses at a rate of 1 NCY per census/calendar-year.

   For rows where imputation is required (nil? NCY) and possible the
   imputed NCY value is returned in the specifiec column and a map
   describing the imputation mapped into a separate column.

   For rows where imputation is not requried (some? NCY) or not possible
   the existing NCY value (which may be nil) is returned in the specified
   column and the imputation description column is left nil.

   Names of columns of `ds` to use (and keys to use within `ncy-imputation`)
   are specified in (optional) options map (or as keyword options) as follows:
  
   `id`                  - unique identifier for the pupil
                           {default :id}
   `cy`                  - calendar|census year
                           {default :calendar-year}
   `ncy`                 - National Curriculum Year
                           {default :academic-year}
   `ncy-imputed`         - column to contain NCYs after imputation
                           (can be the same as `ncy`)
                           {default :academic-year}
   `ncy-imputation`      - imputation description column in returned dataset
                           {default :academic-year-imputation}
   `imputation-from-ncy` - key to use (within `ncy-imputation` map)
                           for reference NCY
                           {default :from-ncy}
   `imputation-from-cy`  - key to use (within `ncy-imputation` map)
                           for reference census/calendar year
                           {default :from-cy}

  NOTE: Row order of ds may NOT be preserved.
  NOTE: Algorithm relies on input dataset `ds` NOT being named \"_ref\".
  "
  ([ds & {:keys [id cy ncy ncy-imputed ncy-imputation imputation-from-cy imputation-from-ncy]
          :or   {id                  :id
                 cy                  :calendar-year
                 ncy                 :academic-year
                 ncy-imputed         :academic-year
                 ncy-imputation      :academic-year-imputation
                 imputation-from-cy  :from-calendar-year
                 imputation-from-ncy :from-academic-year
                 }}]
   (if (zero? (-> ds
                  (tc/drop-missing [id cy ncy])
                  (tc/row-count)))
     (-> ds
         (tc/add-column ncy-imputation nil))
     (-> ds
         (tc/left-join (-> ds
                           (tc/select-columns [id cy ncy])
                           (tc/drop-missing)
                           (tc/order-by       [id cy ncy])
                           (tc/group-by       [id       ])
                           (tc/first)
                           (tc/ungroup)
                           (tc/rename-columns {cy  :cy-ref
                                               ncy :ncy-ref})
                           (tc/set-dataset-name "_ref"))
                       [id])
         (tc/map-columns ncy-imputation [ncy cy :ncy-ref :cy-ref]
                         (fn [ncy cy ncy-ref cy-ref]
                           (when (and (nil? ncy) cy ncy-ref cy-ref)
                             {imputation-from-ncy ncy-ref
                              imputation-from-cy   cy-ref})))
         (tc/map-columns ncy-imputed    [ncy cy :ncy-ref :cy-ref ncy-imputation]
                         (fn [ncy cy ncy-ref cy-ref ncy-imputation]
                           (if (and (nil? ncy) (some? ncy-imputation))
                             (- ncy-ref (- cy-ref cy))
                             ncy)))
         (tc/drop-columns #(= "_ref" %) :src-table-name)))))


(comment
  ;;; # National Curriculum Year Table
  (-> (tc/dataset {:ncy ncys})
      (tc/map-columns :age-at-start-of-school-year [:ncy] #(ncy->age-at-start-of-school-year %))
      (tc/map-columns :age-at-end-of-school-year [:age-at-start-of-school-year] #(inc %))
      (tc/set-dataset-name "National Curriculum Year Table"))
  ;; => National Curriculum Year Table [25 3]:
  ;;    | :ncy | :age-at-start-of-school-year | :age-at-end-of-school-year |
  ;;    |-----:|-----------------------------:|---------------------------:|
  ;;    |   -4 |                            0 |                          1 |
  ;;    |   -3 |                            1 |                          2 |
  ;;    |   -2 |                            2 |                          3 |
  ;;    |   -1 |                            3 |                          4 |
  ;;    |    0 |                            4 |                          5 |
  ;;    |    1 |                            5 |                          6 |
  ;;    |    2 |                            6 |                          7 |
  ;;    |    3 |                            7 |                          8 |
  ;;    |    4 |                            8 |                          9 |
  ;;    |    5 |                            9 |                         10 |
  ;;    |  ... |                          ... |                        ... |
  ;;    |   10 |                           14 |                         15 |
  ;;    |   11 |                           15 |                         16 |
  ;;    |   12 |                           16 |                         17 |
  ;;    |   13 |                           17 |                         18 |
  ;;    |   14 |                           18 |                         19 |
  ;;    |   15 |                           19 |                         20 |
  ;;    |   16 |                           20 |                         21 |
  ;;    |   17 |                           21 |                         22 |
  ;;    |   18 |                           22 |                         23 |
  ;;    |   19 |                           23 |                         24 |
  ;;    |   20 |                           24 |                         25 |
  )


;;; # Tests
;;; ## Tests for ncy->age… and age…->ncy
(rcf/tests "Age at start of reception should be 4 years, per gov.uk/schools-admissions/school-starting-age"
           (ncy->age-at-start-of-school-year 0) := 4)

(rcf/tests
 "Age at start of NCY -4 to 20 should be 0 to 24 respectively"
 (map ncy->age-at-start-of-school-year (range -4 (inc 20))) := (range 0  (inc 24)))

(rcf/tests
 "ncy->age… should return nil for non-integer NCYs outside -4 to 20 inclusive"
 (map ncy->age-at-start-of-school-year [-5 0.5 21]) := '(nil nil nil))

(rcf/tests
 "NCY entered at age 4 should be 0 (reception), per gov.uk/schools-admissions/school-starting-age"
 (age-at-start-of-school-year->ncy 4) := 0)

(rcf/tests
 "NCY entered at age 0 to 24 should be -4 to 20 respectively"
 (map age-at-start-of-school-year->ncy (range  0 (inc 24))) := (range -4 (inc 20)))

(rcf/tests
 "Round trip test: ncy->age… followed by age…->ncy should return the same for integers -4 to 20 inclusive"
 (map (comp age-at-start-of-school-year->ncy ncy->age-at-start-of-school-year) (range -4 (inc 20))) := (range -4 (inc 20)))

(rcf/tests
 "age…->ncy should return nil for non-integer age outside 0 to 24 inclusive"
 (map age-at-start-of-school-year->ncy [-1 0.5 25]) := '(nil nil nil))


;;; ## Tests for impute-nil-ncy
(rcf/tests
 "Where all `ncy` are nil (so no reference rows), should not blow up
 and should return input dataset (including any other columns -
 here :row-id) with add nil `ncy-imputation` column added."
 (let [test-ds (-> (tc/concat (tc/dataset {:id  1 :calendar-year [2000 2001 2002] :academic-year      nil     })
                              (tc/dataset {:id  2 :calendar-year [2000 2001 2002] :academic-year      nil     }))
                   (tc/add-column :row-id (range)))]
   (impute-nil-ncy test-ds) := (tc/add-column test-ds :academic-year-imputation nil)))

(rcf/tests
 "Rows with non-nil `ncy` should be returned as is save for addition
of nil `ncy-imputation` column, regardless of other rows with nil `ncy`."
 (let [test-ds (-> (tc/concat (tc/dataset {:id  2 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                              (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [  0 nil nil]})
                              (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [nil   1 nil]})
                              (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [nil nil   2]})
                              (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1 nil]})
                              (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [nil   1   2]})
                              (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [nil  -4 nil]})
                              (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [nil  20 nil]}))
                   (tc/add-column :row-id (range))
                   (tc/reorder-columns [:row-id]))
       test-rows (into (sorted-set) ((tc/drop-missing test-ds [:academic-year]) :row-id))]
   (-> test-ds
       (impute-nil-ncy)
       (tc/select-rows #(test-rows (:row-id %)))
       (tc/order-by [:row-id]))
   :=
   (-> test-ds
       (tc/select-rows test-rows)
       (tc/select-rows #(test-rows (:row-id %)))
       (tc/add-column :academic-year-imputation nil))))

(rcf/tests
 "For a CYP with a reference row (with non-nil `ncy`), should impute
 `ncy` for any rows with nil `ncy`, otherwise leave as is.
  NOTE: Ignoring `ncy-imputation` column for the moment"
 (-> (tc/concat (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [  0 nil nil]})
                (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [nil   1 nil]})
                (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [nil nil   2]})
                (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1 nil]})
                (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [nil   1   2]}))
     (impute-nil-ncy)
     (tc/drop-columns :academic-year-imputation)
     (tc/order-by [:id :calendar-year])) :=
 (-> (tc/concat (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  7 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]}))))

(rcf/tests
 "Note that `ncy` may be imputed outside the SEND range of -4 to 20"
 (-> (tc/concat (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [nil  -4 nil]})
                (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [nil  20 nil]}))
     (impute-nil-ncy)
     (tc/drop-columns :academic-year-imputation)
     (tc/order-by [:id :calendar-year])) :=
 (-> (tc/concat (tc/dataset {:id  8 :calendar-year [2000 2001 2002] :academic-year [ -5  -4  -3]})
                (tc/dataset {:id  9 :calendar-year [2000 2001 2002] :academic-year [ 19  20  21]}))))

(rcf/tests
 "Where imputation occurs the source should be described in the `:academic-year-imputation` column"
 (-> (tc/dataset {:id 10 :calendar-year [2000 2001] :academic-year [nil   1]})
     (impute-nil-ncy)) :=
 (-> (tc/dataset {:id 10 :calendar-year [2000 2001] :academic-year [  0   1]
                  :academic-year-imputation [{:from-calendar-year 2001
                                              :from-academic-year 1}
                                             nil]})))

(rcf/tests
 "Where column names are specified they should be used, including
 specification of a separate column for the NCY after imputation."
 (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]})
     (impute-nil-ncy {:cy :cy :ncy :ncy
                      :ncy-imputed :ncyi :ncy-imputation :ncyi-desc :imputation-from-cy :from-cy :imputation-from-ncy :from-ncy})) :=
 (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-desc [{:from-cy 2001 :from-ncy 1} nil]})))

(rcf/tests
 "Should also be able to specify options as keyword parameters (rather than a map)."
 (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]})
     (impute-nil-ncy :cy :cy :ncy :ncy
                     :ncy-imputed :ncyi :ncy-imputation :ncyi-desc :imputation-from-cy :from-cy :imputation-from-ncy :from-ncy)) :=
 (-> (tc/dataset {:id 12 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-desc [{:from-cy 2001 :from-ncy 1} nil]})))

(rcf/tests
 "The algorithm should work with non-keyword column & key names"
 (-> (tc/dataset {'id 13 "SEN2 census year" [2000 2001] 'NCY [nil   1]})
     (impute-nil-ncy :id 'id :cy "SEN2 census year" :ncy 'NCY
                     :ncy-imputed 'NCY :ncy-imputation 'ncy-imputation-desc :imputation-from-cy 'from-cy :imputation-from-ncy "from NCY")) :=
 (-> (tc/dataset {'id 13 "SEN2 census year" [2000 2001] 'NCY [  0   1] 'ncy-imputation-desc [{'from-cy 2001 "from NCY" 1} nil]})))

(rcf/tests
 "With multiple possible ref. records, the one with lowest census/calendar-year should be used (:id 21 & 22).
  If there are multiple such records, then the one with smallest NCY is chosen (:id 23).
  Note that if pupils do not progress at a rate of one NCY per census/calendar-year then this can result in
  discontinuities if a CYP repeats or goes back a NCY and has missing NCY in subsequent years (:id 24 & 25)."
 (-> (tc/concat (tc/dataset {:id 21 :cy [2000 2001      2002] :ncy [nil   1       2]})
                (tc/dataset {:id 22 :cy [2000 2001      2002] :ncy [nil   1       1]})
                (tc/dataset {:id 23 :cy [2000 2001 2001     ] :ncy [nil   1   0    ]})
                (tc/dataset {:id 24 :cy [2000 2001      2002] :ncy [  1   1     nil]})
                (tc/dataset {:id 25 :cy [2000 2001      2002] :ncy [  1   0     nil]}))
     (impute-nil-ncy :cy :cy :ncy :ncy :ncy-imputed :ncy
                     :ncy-imputation :ncy-imputation :imputation-from-cy :from-cy :imputation-from-ncy :from-ncy)) :=
 (-> (tc/concat (tc/dataset {:id 21 :cy [2000 2001      2002] :ncy [  0   1       2] :ncy-imputation [{:from-cy 2001 :from-ncy 1} nil nil]})
                (tc/dataset {:id 22 :cy [2000 2001      2002] :ncy [  0   1       1] :ncy-imputation [{:from-cy 2001 :from-ncy 1} nil nil]})
                (tc/dataset {:id 23 :cy [2000 2001 2001     ] :ncy [ -1   1   0    ] :ncy-imputation [{:from-cy 2001 :from-ncy 0} nil nil]})
                (tc/dataset {:id 24 :cy [2000 2001      2002] :ncy [  1   1       3] :ncy-imputation [nil nil {:from-cy 2000 :from-ncy 1}]})
                (tc/dataset {:id 25 :cy [2000 2001      2002] :ncy [  1   0       3] :ncy-imputation [nil nil {:from-cy 2000 :from-ncy 1}]}))))
