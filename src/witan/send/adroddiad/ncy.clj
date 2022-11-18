(ns witan.send.adroddiad.ncy
  (:require [tablecloth.api :as tc]
            [hyperfiddle.rcf :as rcf]))



(defn age-at-start-of-school-year->ncy [age]
  (- age 4))



(defn ncy->age-at-start-of-school-year [ncy]
  (+ ncy 4))



(defn impute-nil-ncy
  "Impute values for National Curriculum Year (NCY) where nil in `ds`.

   Imputation is performed for pupils with at least one record with a
   non-nil NCY and non-nil census/calendar year, using the first such
   pair (with smallest NCY) as a reference point from which to calculate
   the nil NCYs, assuming 1 NCY per census/calendar-year. For rows where
   imputation is possible the nil NCY is replaced with the imputed value
   and a string describing the imputation mapped into a separate
   column. For rows where imputation is not possible NCY is left nil.

   Columns of `ds` to use are specified in (optional) map as follows:
   `id`             - unique identifier for the pupil {default :id}
   `cy`             - calendar|census year of the row {default :calendar-year}
   `ncy`            - National Curriculum Year of the row {default :academic-year}
   `ncy-imputation` - imputation description column in returned dataset {default :academic-year-imputation}"
  ([ds]
   (impute-nil-ncy ds {}))
  ([ds {:keys [id cy ncy ncy-imputed ncy-imputation]
        :or   {id             :id
               cy             :calendar-year
               ncy            :academic-year
               ncy-imputed    :academic-year
               ncy-imputation :academic-year-imputation}}]
   (-> ds
       (tc/left-join (-> ds
                         (tc/select-columns [id ncy cy])
                         (tc/drop-missing)
                         (tc/order-by       [id ncy cy])
                         (tc/group-by       [id       ])
                         (tc/first)
                         (tc/ungroup)
                         (tc/rename-columns {ncy :ncy-ref
                                             cy  :ref-cy})
                         (tc/set-dataset-name "_ref"))
                     [id])
       (tc/map-columns ncy-imputation [ncy :ncy-ref cy :ref-cy ]
                       (fn [ncy ncy-ref cy cy-ref]
                         (when (and (nil? ncy) (some? ncy-ref) (some? cy) (some? cy-ref))
                           (str "Imputed from NCY=" ncy-ref " in " cy-ref ))))
       (tc/map-columns ncy-imputed    [ncy :ncy-ref cy :ref-cy ncy-imputation]
                       (fn [ncy ncy-ref cy cy-ref ncy-imputation]
                         (if (and (nil? ncy) (some? ncy-imputation))
                           (- ncy-ref (- cy-ref cy))
                           ncy)))
       (tc/drop-columns #(= "_ref" %) :src-table-name))))

(comment
  ;; Checks & tests:
  ;; - default column names
  ;; - imputation cases
  ;;   - no imputation needed
  ;;   - single record nil NCY
  ;;   - multiple records nil NCY
  ;;   - single record nil NCY, single ref record
  ;;     - CY for nil NCY < that of ref record
  ;;     - CY for nil NCY > that of ref record
  ;;     - CY for nil NCY cf. ref record such that NCY imputes <0
  ;;     - CY for nil NCY cf. ref record such that NCY imputes >20
  ;;   - multiple record nil NCY, multiple ref record
  ;;     - nil NCY interlaced with ref records
  ;; - existing ncy-imputation column with non-nil values?
  ;; - different column names

  )



(comment

  )



(rcf/tests

 "Age at start of reception should be 4, per SEN2 guidelines"
 (ncy->age-at-start-of-school-year 0) := 4

 [1 2 3] := (take 3 [1 2 3 4])

 )

