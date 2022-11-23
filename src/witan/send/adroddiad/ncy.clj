(ns witan.send.adroddiad.ncy
  (:require [tablecloth.api :as tc]
            [hyperfiddle.rcf :as rcf]))


(def ncy->age-at-start-of-school-year 
  "Maps national curriculum year (NCY) to age in whole number of years
  on 31st August prior to starting the school/academic year.

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  NCYs and ages for reception to NCY 11 (children aged 4 to 15 at the
  start of school year) are per https://www.gov.uk/national-curriculum.
  Extension to NCYs -4 to -1 (ages 0-3) and NCYs 12 to 20, (ages 16-24)
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic year is +4.

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year. This is per:

  - The [gov.uk website](https://www.gov.uk/schools-admissions/school-starting-age)
  which (as of 23-NOV-2022) states:
  \"Most children start school full-time in the September after their
  fourth birthday. This means they’ll turn 5 during their first school
  year. For example, if your child’s fourth birthday is between 1
  September 2021 and 31 August 2022 they will usually start school in
  September 2022.\".

  - The [2022 SEN2 guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf)
  which states in section 1.1 that: \"age breakdown refers to age as
  at 31 August 2021\", implying (since this is for the January 2022
  SEN2 return) that age for SEN2 breakdowns is as of 31-AUG prior to
  starting the school year.

  The maximum age for SEND is 25, but per the relevant legistlation \"a
  local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum NCY in
  this map is 20 (age 24 at start of school/academic year).
  "
  (let [ncy (range -4 (inc 20))]
    (apply sorted-map (interleave ncy (map #(+ % 4) ncy)))))



(def age-at-start-of-school-year->ncy
  "Maps age in whole number of years on 31st August prior to starting
  the school/academic year to the corresponding national curriculum year (NCY).

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  Ages and NCYs for children aged 4 to 15 at the start of school year
  (reception to NCY 11) are per https://www.gov.uk/national-curriculum.
  Extension to ages 0-3 (NCYs -4 to -1) and ages 16-24 (NCYs 12 to 20),
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic year is -4.

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year. This is per:

  - The [gov.uk website](https://www.gov.uk/schools-admissions/school-starting-age)
  which (as of 23-NOV-2022) states:
  \"Most children start school full-time in the September after their
  fourth birthday. This means they’ll turn 5 during their first school
  year. For example, if your child’s fourth birthday is between 1
  September 2021 and 31 August 2022 they will usually start school in
  September 2022.\".

  - The [2022 SEN2 guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf)
  which states in section 1.1 that: \"age breakdown refers to age as
  at 31 August 2021\", implying (since this is for the January 2022
  SEN2 return) that age for SEN2 breakdowns is as of 31-AUG prior to
  starting the school year.

  The maximum age for SEND is 25, but per the relevant legistlation \"a
  local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum age in
  this map is 24 (NCY 20).
  "
  (let [age (range 0 (inc 24))]
    (apply sorted-map (interleave age (map #(- % 4) age)))))



(rcf/tests

 "Age at start of reception should be 4 years, per gov.uk/schools-admissions/school-starting-age"
 (ncy->age-at-start-of-school-year 0) := 4

 "Age at start of NCY -4 to 20 should be 0 to 24"
 (map ncy->age-at-start-of-school-year (range -4 (inc 20))) := (range 0  (inc 24))

 "ncy->age… should return nil for non-integer NCYs outside -4 to 20 inclusive"
 (map ncy->age-at-start-of-school-year [-5 0.5 21]) := '(nil nil nil)

 "NCY entered at age 4 should be 0 (reception), per gov.uk/schools-admissions/school-starting-age"
 (age-at-start-of-school-year->ncy 4) := 0

 "NCY entered at age 0 to 24 should be -4 to 20"
 (map age-at-start-of-school-year->ncy (range  0 (inc 24))) := (range -4 (inc 20))
 
 "Round trip test: ncy->age… followed by age…->ncy should return the same for integers -4 to 20 inclusive"
 (map (comp age-at-start-of-school-year->ncy ncy->age-at-start-of-school-year) (range -4 (inc 20))) := (range -4 (inc 20))
 
 "age…->ncy should return nil for non-integer age outside 0 to 24 inclusive"
 (map age-at-start-of-school-year->ncy [-1 0.5 25]) := '(nil nil nil)
 
 )



(defn impute-nil-ncy
  "Impute values for National Curriculum Year (NCY) where nil in `ds`.

   Imputation is performed for rows with a nil NCY for pupils with at
   least one other row with a non-nil NCY and non-nil census/calendar
   year, using the first such pair (with smallest census/calendar year)
   as a reference point from which to calculate the nil NCYs, assuming
   1 NCY per census/calendar-year.

   For rows where imputation is required (nil? NCY) and possible the
   imputed NCY value is returned in the specifiec column and a string
   describing the imputation mapped into a separate column.

   For rows where imputation is not requried (some? NCY) or not possible
   the existing NCY value (which may be nil) is returned in the specified
   column and the imputation description column is left nil.

   Columns of `ds` to use are specified in (optional) map as follows:
   `id`             - unique identifier for the pupil {default :id}
   `cy`             - calendar|census year of the row {default :calendar-year}
   `ncy`            - National Curriculum Year of the row {default :academic-year}
   `ncy-imputed`    - column to contain NCYs after imputation {default :academic-year}
   `ncy-imputation` - imputation description column in returned dataset {default :academic-year-imputation}

  NOTE: row order of ds is NOT preserved.
  NOTE: Algorithm relies on input dataset NOT being named \"_ref\".
  "
  ([ds & {:keys [id cy ncy ncy-imputed ncy-imputation]
          :or   {id             :id
                 cy             :calendar-year
                 ncy            :academic-year
                 ncy-imputed    :academic-year
                 ncy-imputation :academic-year-imputation}}]
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
                           (when (and (nil? ncy) (some? cy) (some? ncy-ref) (some? cy-ref))
                             (str "Imputed from NCY=" ncy-ref " in " cy-ref ))))
         (tc/map-columns ncy-imputed    [ncy cy :ncy-ref :cy-ref ncy-imputation]
                         (fn [ncy cy ncy-ref cy-ref ncy-imputation]
                           (if (and (nil? ncy) (some? ncy-imputation))
                             (- ncy-ref (- cy-ref cy))
                             ncy)))
         (tc/drop-columns #(= "_ref" %) :src-table-name)))))


(rcf/tests

 "Where all :academic-year are nil (so no reference rows), should just add nil :academic-year column."
 (-> (tc/dataset {:id [-1 -0] :calendar-year 2000 :academic-year nil})
     (impute-nil-ncy)) :=
 (-> (tc/dataset {:id [-1 -0] :calendar-year 2000 :academic-year nil :academic-year-imputation nil}))


 "For a subject with a reference row (`:id`s 2 to 5 here), imputes NCY for any rows with nil NCY, otherwise leaves as is.
   NOTE: NCY may be imputed outside the SEND range of -4 to 20.
   NOTE: Ignoring :academic-year-imputation column for the moment"
 (-> (tc/concat (tc/dataset {:id  1 :calendar-year [2000 2001 2002] :academic-year      nil     })
                (tc/dataset {:id  2 :calendar-year [2000 2001 2002] :academic-year [nil   1 nil]})
                (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [nil nil   2]})
                (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [nil nil  -4]})
                (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [ 20 nil nil]})
                (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]}))
     (impute-nil-ncy)
     (tc/drop-columns :academic-year-imputation)
     (tc/order-by [:id :calendar-year])) :=
 (-> (tc/concat (tc/dataset {:id  1 :calendar-year [2000 2001 2002] :academic-year      nil     })
                (tc/dataset {:id  2 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  3 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})
                (tc/dataset {:id  4 :calendar-year [2000 2001 2002] :academic-year [ -6  -5  -4]})
                (tc/dataset {:id  5 :calendar-year [2000 2001 2002] :academic-year [ 20  21  22]})
                (tc/dataset {:id  6 :calendar-year [2000 2001 2002] :academic-year [  0   1   2]})))

 
 "Where imputation occurs the source should be described in the `:academic-year-imputation` column"
 (-> (tc/dataset {:id  7 :calendar-year [2000 2001] :academic-year [nil   1]})
     (impute-nil-ncy)) :=
 (-> (tc/dataset {:id  7 :calendar-year [2000 2001] :academic-year [  0   1] :academic-year-imputation ["Imputed from NCY=1 in 2001" nil]}))
 
 
 "Where column names are specified they should be used, including specification of a separate column for the NCY after imputation."
 (-> (tc/dataset {:id  8 :cy [2000 2001] :ncy [nil   1]})
     (impute-nil-ncy {:cy :cy :ncy :ncy :ncy-imputed :ncyi :ncy-imputation :ncyi-desc})) :=
 (-> (tc/dataset {:id  8 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-desc ["Imputed from NCY=1 in 2001" nil]}))

 "Note can also specify column names as keyword parameters (rather than a map)"
 (-> (tc/dataset {:id  8 :cy [2000 2001] :ncy [nil   1]})
     (impute-nil-ncy :cy :cy :ncy :ncy :ncy-imputed :ncyi :ncy-imputation :ncyi-desc)) :=
 (-> (tc/dataset {:id  8 :cy [2000 2001] :ncy [nil   1]  :ncyi [  0   1] :ncyi-desc ["Imputed from NCY=1 in 2001" nil]}))

 
 "With multiple possible ref. records, the one with lowest census/calendar-year should be used (:id 9 & 10).
  If there are multiple such records, then the one with smallest NCY is chosen (:id 11).
  Note that if pupils do not progress at a rate of one NCY per census/calendar-year then this can result in
  discontinuities if a CYP repeats or goes back a NCY and has missing NCY in subsequent years (:id 12 & 13)."
 (-> (tc/concat (tc/dataset {:id  9 :cy [2000 2001      2002] :ncy [nil   1   2]})
                (tc/dataset {:id 10 :cy [2000 2001      2002] :ncy [nil   1   0]})
                (tc/dataset {:id 11 :cy [2000 2001 2001     ] :ncy [nil   1   0]})
                (tc/dataset {:id 12 :cy [2000 2001      2002] :ncy [  1   1 nil]})
                (tc/dataset {:id 13 :cy [2000 2001      2002] :ncy [  1   0 nil]}))
     (impute-nil-ncy :cy :cy :ncy :ncy :ncy-imputed :ncy :ncy-imputation :ncy-imputation)) :=
 (-> (tc/concat (tc/dataset {:id  9 :cy [2000 2001      2002] :ncy [  0   1   2] :ncy-imputation ["Imputed from NCY=1 in 2001" nil nil]})
                (tc/dataset {:id 10 :cy [2000 2001      2002] :ncy [  0   1   0] :ncy-imputation ["Imputed from NCY=1 in 2001" nil nil]})
                (tc/dataset {:id 11 :cy [2000 2001 2001     ] :ncy [ -1   1   0] :ncy-imputation ["Imputed from NCY=0 in 2001" nil nil]})
                (tc/dataset {:id 12 :cy [2000 2001      2002] :ncy [  1   1   3] :ncy-imputation [nil nil "Imputed from NCY=1 in 2000"]})
                (tc/dataset {:id 13 :cy [2000 2001      2002] :ncy [  1   0   3] :ncy-imputation [nil nil "Imputed from NCY=1 in 2000"]})))
 
 )
