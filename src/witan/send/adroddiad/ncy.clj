(ns witan.send.adroddiad.ncy
  "Definitions and functions for handling National Curriculum Year"
  (:require [clojure.set :as set]
            [tablecloth.api :as tc]))



;;; # Utility functions
(defn- inclusive-range
  "Returns a lazy seq of nums from `start` (inclusive) to `end` (inclusive), by step 1"
  [start end]
  (range start (inc end)))



;;; # Age at the start of the school year
(defn age-at-start-of-school-year-for-census-year
  "Age on 31st August on year prior to `cy`, in completed years,
  for child with date of birth `dob` or year & month of birth `dob-year` & `dob-month`.

  NOTE: Age is on 31st August *prior* to `cy`:
        - E.g. for `cy`=2020, age is calculated on 31-Aug-2019, i.e. at the start of the 2019/20 school year.
        - The use of the second calendar year of the school year reflects the timing of the SEN2 census in January.

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year, per:

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
  "
  ([cy dob] (age-at-start-of-school-year-for-census-year cy (.getYear dob) (.getMonthValue dob)))
  ([cy dob-year dob-month] (- cy 1 dob-year (if (< 8 dob-month) 1 0))))

(defn age-at-start-of-school-year-for-date
  "Age on 31st August prior to `date` for child with date of birth `dob`.

  `dob` & `date` should be java.time.LocalDate objects

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year, per:

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
  31-AUG prior to starting the school year."
  [date dob]
  (- (- (.getYear date) (if (< (.getMonthValue date) 9) 1 0))
     (- (.getYear dob)  (if (< (.getMonthValue dob)  9) 1 0))
     1))



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

  The maximum age for SEND is 25, but per the relevant legislation
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

  The maximum age for SEND is 25, but per the relevant legislation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum age in
  this map is 24 (NCY 20).
  "
  (set/map-invert ncy->age-at-start-of-school-year))


;;; ## National Curriculum Year Table
(comment
  (-> (tc/dataset {:ncy ncys})
      (tc/map-columns :age-at-start-of-school-year [:ncy] ncy->age-at-start-of-school-year)
      (tc/map-columns :age-at-end-of-school-year [:age-at-start-of-school-year] inc)
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



;;; # Imputation for missing National Curriculum Year
(defn ds->ref-cy-ncy-map
  "Given dataset `ds` containing `id`, `cy` and `ncy` columns,
  returns map mapping `id`s that have at least one record with non-missing `cy` and `ncy`
  to a map with keys `ref-cy` & `ref-ncy` containing the first `cy` and `ncy` for that `id`."
  [ds & {:keys [id cy ncy ref-cy ref-ncy]
         :or   {id  :id
                cy  :calendar-year
                ncy :academic-year}}]
  (let [ref-cy  (or ref-cy  cy)
        ref-ncy (or ref-ncy ncy)]
    (-> ds
        (tc/select-columns [id cy ncy])
        tc/drop-missing
        ((fn [ds]
           (if (zero? (tc/row-count ds))
             {}
             (-> ds
                 (tc/order-by [id cy ncy])
                 (tc/group-by [id])
                 tc/first
                 tc/ungroup
                 (tc/rename-columns {cy ref-cy, ncy ref-ncy})
                 (#(zipmap (-> % (get id))
                           (-> % (tc/drop-columns [id]) (tc/rows :as-maps)))))))))))

(defn impute-nil-ncy
  "Impute values for National Curriculum Year (NCY) where nil in `ds`.

   Imputation is performed for rows with a nil NCY for pupils with at
   least one other row with a non-nil census/calendar year non-nil NCY,
   using the first such pair (with smallest census/calendar year)
   as a reference point from which to calculate the nil NCYs, assuming
   the CYP progresses at a rate of 1 NCY per census/calendar-year.

   For rows where imputation is required (nil? NCY) and possible the
   imputed NCY value is returned in the specifiec column and a map
   containing the reference census/calendar-year and NCY included in an
   (optional) separate column.

   For rows where imputation is not requried (some? NCY) or not possible
   (no reference census/calendar-year & NCY) the existing NCY value (which
   may be nil) is returned in the specified column and the (optional)
   imputation reference column is left nil.

   Names of columns of `ds` to use (and keys to use within `ncy-imputation`)
   are specified in (optional) options map (or as keyword options) as follows:
  
   `id`               - unique identifier for the pupil
                        {default :id}
   `cy`               - calendar|census year
                        {default :calendar-year}
   `ncy`              - National Curriculum Year
                        {default :academic-year}
   `ncy-imputed`      - column to contain NCYs after imputation
                        (can be the same as `ncy`)
                        {default :academic-year}
   `ncy-imputed-from` - name for column to contain imputation reference in returned dataset
                        (specify as `nil` to omit)
                        {default `nil`}
   `ref-cy`           - key to use (within `ncy-imputed-from` map)
                        for reference census/calendar year
                        {default `cy`}
   `ref-ncy`          - key to use (within `ncy-imputed-from` map)
                        for reference NCY
                        {default `ncy`}"
  ([ds & {:keys [id cy ncy ncy-imputed ncy-imputed-from ref-cy ref-ncy]
          :or   {id               :id
                 cy               :calendar-year
                 ncy              :academic-year
                 ncy-imputed-from nil}}]
   (let [ncy-imputed    (or ncy-imputed ncy)
         ref-cy         (or ref-cy       cy)
         ref-ncy        (or ref-ncy     ncy)
         id->ref-cy-ncy (ds->ref-cy-ncy-map ds {:id id, :cy cy, :ncy ncy, :ref-cy ref-cy, :ref-ncy ref-ncy})]
     (tc/map-rows ds (fn [{id-val id, cy-val cy, ncy-val ncy}]
                       (let [{ref-cy-val  ref-cy
                              ref-ncy-val ref-ncy, :as ref} (get id->ref-cy-ncy id-val)]
                         (-> (if (and (nil? ncy-val) cy-val ref-cy-val ref-ncy-val)
                               {ncy-imputed      (+ ref-ncy-val (- cy-val ref-cy-val))
                                ncy-imputed-from ref}
                               {ncy-imputed      ncy-val
                                ncy-imputed-from nil})
                             (dissoc nil))))))))
