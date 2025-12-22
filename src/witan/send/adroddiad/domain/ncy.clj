(ns witan.send.adroddiad.domain.ncy
  "Definitions and functions for handling National Curriculum Year"
  (:require [clojure.set :as set]
            [tablecloth.api :as tc]))



;;; # Utility functions
(defn- inclusive-range
  "Returns a lazy seq of nums from `start` (inclusive) to `end` (inclusive), by step 1"
  [start end]
  (range start (inc end)))

(defn key-comparator-fn-by
  [key->order]
  (fn [k1 k2]
    (compare [(key->order k1) k1]
             [(key->order k2) k2])))



;;; # Age at the start of the school/academic/scholastic year
(defn age-at-start-of-school-year-for-census-year
  "Age on 31st August in calendar year prior to `cy`, in completed years,
  for child with date of birth `dob` or year & month of birth `dob-year` & `dob-month`.

  NOTE: Age is on 31st August in calendar year *prior* to `cy`:
        - E.g. for `cy`=2020, age is calculated on 31-Aug-2019, i.e. at the start of the 2019/20 school year.
        - The use of the second calendar year of the school year reflects the timing of the SEN2 census in January.

  Age at the start of the school/academic/scholastic year is the age on 31st August
  prior to the school/academic/scholastic year, per:

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
  the January 2022 SEN2 return) that age for SEN2 breakdowns is age on
  the 31-AUG prior to the school/academic/scholastic year.
  "
  ([cy dob] (age-at-start-of-school-year-for-census-year cy (.getYear dob) (.getMonthValue dob)))
  ([cy dob-year dob-month] (- cy 1 dob-year (if (< 8 dob-month) 1 0))))

(defn age-at-start-of-school-year-for-date
  "Age on 31st August prior to `date` for child with date of birth `dob`.

  `dob` & `date` should be java.time.LocalDate objects

  Age at the start of the school/academic/scholastic year is the age on 31st August
  prior to the school/academic/scholastic year, per:

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
  the January 2022 SEN2 return) that age for SEN2 breakdowns is age on
  the 31-AUG prior to the school/academic/scholastic year."
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
  on 31st August prior to starting the school/academic/scholastic year.

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  NCYs and ages for reception to NCY 11 (children aged 4 to 15 at the
  start of the school/academic/scholastic year) are per https://www.gov.uk/national-curriculum.
  Extension to NCYs -4 to -1 (ages 0-3) and NCYs 12 to 20, (ages 16-24)
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic/scholastic year is +4.

  The maximum age for SEND is 25, but per the relevant legislation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic/scholastic year, hence the maximum NCY in
  this map is 20 (age 24 at start of school/academic/scholastic year).
  "
  (apply sorted-map (interleave ncys (map #(+ % 4) ncys))))

(def age-at-start-of-school-year->ncy
  "Maps age in whole number of years on 31st August prior to starting
  the school/academic/scholastic year to the corresponding national curriculum year (NCY).

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  Ages and NCYs for children aged 4 to 15 at the start of school/academic/scholastic
  year (reception to NCY 11) are per https://www.gov.uk/national-curriculum.
  Extension to ages 0-3 (NCYs -4 to -1) and ages 16-24 (NCYs 12 to 20),
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic/scholastic year is -4.

  The maximum age for SEND is 25, but per the relevant legislation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic/scholastic year, hence the maximum age in
  this map is 24 (NCY 20).
  "
  (->> ncy->age-at-start-of-school-year
       set/map-invert
       (into (sorted-map))))



;;; # National Curriculum Years dictionary
(def dataset
  "National Curriculum Year definitions as a dataset."
  (-> (tc/dataset {:ncy ncys})
      (tc/map-columns :abbreviation [:ncy] identity) ; Note: integer rather than string.
      (tc/map-columns :order [:ncy] identity) ; Note: numbering as NCY rather than from 0.
      (tc/map-columns :age-at-start-of-school-year [:ncy] ncy->age-at-start-of-school-year)
      (tc/map-columns :age-at-end-of-school-year [:age-at-start-of-school-year] inc)
      (tc/map-rows (fn [{:keys [ncy
                                age-at-start-of-school-year
                                age-at-end-of-school-year]}]
                     (let [name       (cond
                                        ((set (inclusive-range -4 -3)) ncy)
                                        (format "Early Years %d" (+ ncy 5))
                                        ((set (inclusive-range -2 -1)) ncy)
                                        (format "Nursery %d" (+ ncy 3))
                                        ((set (inclusive-range  0  0)) ncy)
                                        "Reception"
                                        ((set (inclusive-range  1 11)) ncy)
                                        (format "Year %d" ncy)
                                        ((set (inclusive-range 12 14)) ncy)
                                        (format "Year %d" ncy)
                                        ((set (inclusive-range 15 20)) ncy)
                                        (format "Age %d–%d" age-at-start-of-school-year age-at-end-of-school-year))
                           label      name
                           definition (format (str "Aged %d on 31st August "
                                                   "prior to the school/academic/scholastic year, "
                                                   "so reach age %d by the end of the year.")
                                              age-at-start-of-school-year
                                              age-at-end-of-school-year)]
                       {:name       name
                        :label      label
                        :definition definition})))
      (tc/reorder-columns [:abbreviation :ncy :order :name :label
                           :age-at-start-of-school-year
                           :age-at-end-of-school-year
                           :definition])
      (tc/order-by [:order])
      (tc/set-dataset-name "ncys")))

(comment ;; EDA: NCY dictionary `dataset` structure:
  (-> dataset
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max]))
  ;;=> ncys: descriptive-stats [8 6]:
  ;;   
  ;;   |                    :col-name | :datatype | :n-valid | :n-missing | :min | :max |
  ;;   |------------------------------|-----------|---------:|-----------:|-----:|-----:|
  ;;   |                :abbreviation |    :int64 |       25 |          0 | -4.0 | 20.0 |
  ;;   |                         :ncy |    :int64 |       25 |          0 | -4.0 | 20.0 |
  ;;   |                       :order |    :int64 |       25 |          0 | -4.0 | 20.0 |
  ;;   |                        :name |   :string |       25 |          0 |      |      |
  ;;   |                       :label |   :string |       25 |          0 |      |      |
  ;;   | :age-at-start-of-school-year |    :int64 |       25 |          0 |  0.0 | 24.0 |
  ;;   |   :age-at-end-of-school-year |    :int64 |       25 |          0 |  1.0 | 25.0 |
  ;;   |                  :definition |   :string |       25 |          0 |      |      |
  ;;   
  
  :rcf)

  (comment ;; EDA: NCY dictionary `dataset`:
  (-> dataset
      (tc/drop-columns [:definition])
      (vary-meta assoc :print-index-range 1000))
  ;;=> ncys [25 7]:
  ;;   
  ;;   | :abbreviation | :ncy | :order |         :name |        :label | :age-at-start-of-school-year | :age-at-end-of-school-year |
  ;;   |--------------:|-----:|-------:|---------------|---------------|-----------------------------:|---------------------------:|
  ;;   |            -4 |   -4 |     -4 | Early Years 1 | Early Years 1 |                            0 |                          1 |
  ;;   |            -3 |   -3 |     -3 | Early Years 2 | Early Years 2 |                            1 |                          2 |
  ;;   |            -2 |   -2 |     -2 |     Nursery 1 |     Nursery 1 |                            2 |                          3 |
  ;;   |            -1 |   -1 |     -1 |     Nursery 2 |     Nursery 2 |                            3 |                          4 |
  ;;   |             0 |    0 |      0 |     Reception |     Reception |                            4 |                          5 |
  ;;   |             1 |    1 |      1 |        Year 1 |        Year 1 |                            5 |                          6 |
  ;;   |             2 |    2 |      2 |        Year 2 |        Year 2 |                            6 |                          7 |
  ;;   |             3 |    3 |      3 |        Year 3 |        Year 3 |                            7 |                          8 |
  ;;   |             4 |    4 |      4 |        Year 4 |        Year 4 |                            8 |                          9 |
  ;;   |             5 |    5 |      5 |        Year 5 |        Year 5 |                            9 |                         10 |
  ;;   |             6 |    6 |      6 |        Year 6 |        Year 6 |                           10 |                         11 |
  ;;   |             7 |    7 |      7 |        Year 7 |        Year 7 |                           11 |                         12 |
  ;;   |             8 |    8 |      8 |        Year 8 |        Year 8 |                           12 |                         13 |
  ;;   |             9 |    9 |      9 |        Year 9 |        Year 9 |                           13 |                         14 |
  ;;   |            10 |   10 |     10 |       Year 10 |       Year 10 |                           14 |                         15 |
  ;;   |            11 |   11 |     11 |       Year 11 |       Year 11 |                           15 |                         16 |
  ;;   |            12 |   12 |     12 |       Year 12 |       Year 12 |                           16 |                         17 |
  ;;   |            13 |   13 |     13 |       Year 13 |       Year 13 |                           17 |                         18 |
  ;;   |            14 |   14 |     14 |       Year 14 |       Year 14 |                           18 |                         19 |
  ;;   |            15 |   15 |     15 |     Age 19–20 |     Age 19–20 |                           19 |                         20 |
  ;;   |            16 |   16 |     16 |     Age 20–21 |     Age 20–21 |                           20 |                         21 |
  ;;   |            17 |   17 |     17 |     Age 21–22 |     Age 21–22 |                           21 |                         22 |
  ;;   |            18 |   18 |     18 |     Age 22–23 |     Age 22–23 |                           22 |                         23 |
  ;;   |            19 |   19 |     19 |     Age 23–24 |     Age 23–24 |                           23 |                         24 |
  ;;   |            20 |   20 |     20 |     Age 24–25 |     Age 24–25 |                           24 |                         25 |
  ;;   
  
  :rcf)

(def dictionary
  "National Curriculum Year dictionary"
  (-> dataset
      ((fn [ds] (zipmap (-> ds :abbreviation)
                        (-> ds (tc/drop-columns [:abbreviation]) (tc/rows :as-maps)))))
      (as-> $ (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $))))

(def abbreviations
  "National Curriculum Year abbreviations as a sorted set"
  ;; Note: Same as `ncys` above but included with this derivation
  ;; for consistency with other domain namespaces.
  (into (sorted-set-by (key-comparator-fn-by (update-vals dictionary :order)))
        (keys dictionary)))

(comment ;; CHECK: `abbreviations` the same as `ncys`
  (= ncys abbreviations)
  ;;=> true
  
  :rcf)



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
