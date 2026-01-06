(ns witan.send.adroddiad.domain.school-phase
  "Definitions and functions for handling Mastodon C's definitions of school phase for SEND reporting and planning:
   Note that:
   - early-childhood is everything before starting school in reception
   - primary         includes reception
   - secondary       ends at NCY 11 (age 15-16), and excludes sixth-form
   - education after secondary is split into age 16-19 and 19-25,
     where the ages are the nominal ages for the NCYs included†, 
     with the ranges indicating the minimum age on 31st August prior to the start of the school year 
     and the maximum age on 31st August after the end of the school year, 
     (to match the description of age for NCYs used by DfE, e.g. on www.gov.uk/national-curriculum):
     - age-16-19: NCYs 12–14 (corresponding to age 16–18 on 31st August prior to the start of the school year†)
     - age-19-25: NCYs 15–20 (corresponding to age 19–24 on 31st August prior to the start of the school year†)
     …with age-16-19 being 3 school years (rather than the 2 of a sixth-form)
      matching the right of CYP to 3 years of post secondary education.
   † For details and mapping of age to nominal NCY, see `witan.send.adroddiad.domain.ncy`."
  (:require [tablecloth.api :as tc]))



;;; # Utility functions
(defn key-comparator-fn-by
  [key->order]
  (fn [k1 k2]
    (compare [(key->order k1) k1]
             [(key->order k2) k2])))



;;; # Mastodon C School Phases
;; Note that in contrast to school phases previously defined in `witan.send.domain.academic-years`:
;; - Using "early-childhood" for NCYs #{-4 -3 -2 -1} (as recommended by our advisor 11-NOV-22),
;;   rather than "nursery" or "early-years" to avoid confusion:
;;   - Nursery schools are "aimed at pre-school children aged three and four years old"
;;     (according to the [Early Years Alliance](https://www.eyalliance.org.uk/how-choose-right-childcare-and-early-education)).
;;   - Early years as used in [national curriculum](https://www.gov.uk/national-curriculum) is NCYs #{-1 0}.
;;   - Early years as used in [early years foundation stage (EYFS)](https://www.gov.uk/early-years-foundation-stage)
;;     is "from birth to 5 years old" which includes reception.
;; - Keys are abbreviations rather than keywords, to facilitate use in datasets & files as strings.
;; - Abbreviations are hyphenated so "keyword friendly" and can be converted to keywords if required.
;; - The map is sorted so that `(keys dictionary)` returns the school phases in the correct order.
;; Note that in contrast to the previous implementation in `witan.send.adroddiad.school-phase`:
;; - domain specific naming (e.g. `school-phases`) has been replaced by generic naming (e.g. `dictionary`).
;; - "post-16" & "post-19" have been renamed "age-16-19" & "age-19-25" to avoid the risk of "post-16"
;;   being interpreted as age 16-25 (NCYs 12-20).

(def dictionary
  "Mastodon C School Phase dictionary"
  (-> {"early-childhood" {:order      0
                          :name       "Early Childhood Education and Care"
                          :label      "Early Childhood"
                          :definition "Prior to starting school: Age 0-4"
                          :ncy-from   -4 ;; Note witan.send.domain.academic-years/nursery includes -5
                          :ncy-to     -1}
       "primary"         {:order      1
                          :name       "Primary"
                          :label      "Primary"
                          :definition "Primary school: Reception and Key Stages 1 & 2 - Reception + NCYs 1 to 6"
                          :ncy-from   0
                          :ncy-to     6}
       "secondary"       {:order      2
                          :name       "Secondary"
                          :label      "Secondary"
                          :definition "Secondary school: Key Stages 3 & 4 - NCYs 7 to 11"
                          :ncy-from   7
                          :ncy-to     11}
       "age-16-19"       {:order      3
                          :name       "Age 16 to 19"
                          :label      "Age 16 to 19"
                          :definition (str "Age 16 (on 31st August prior to the school year) "
                                           "to 19 (on 31st August after the school year) "
                                           "- Key Stage 5 "
                                           "- NCYs 12 to 14")
                          :ncy-from   12
                          :ncy-to     14}
       "age-19-25"       {:order      4
                          :name       "Age 19 to 25"
                          :label      "Age 19 to 25"
                          :definition (str "Age 19 (on 31st August prior to the school year) "
                                           "to 25 (on 31st August after the school year) "
                                           "- Post Key Stage 5 up to 25 years of age "
                                           "- NCYs 15 to 20")
                          :ncy-from   15
                          :ncy-to     20}}
      (update-vals (fn [m] (assoc m :ncys (into (sorted-set) (range (:ncy-from m) (inc (:ncy-to m)))))))
      (as-> $ (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $))))

(def ^:deprecated school-phases dictionary)

(def abbreviations
  "Mastodon C School Phase abbreviations as a sorted set"
  (into (sorted-set-by (key-comparator-fn-by (update-vals dictionary :order)))
        (keys dictionary)))

(def dataset
  "Mastodon C School Phase definitions as a dataset."
  (as-> dictionary $
    (map (fn [[k v]] (assoc v :abbreviation k)) $)
    (tc/dataset $)
    (tc/reorder-columns $ [:abbreviation])
    (tc/set-dataset-name $ "school-phases")))

(def ^:deprecated school-phases-ds dataset)



;;; # Functions to manipulate school-phases
(defn ncy->school-phase-map
  "Given [optional] `dictionary` (defaults to namespace definition) mapping each
   school-phase to a map containing a `:ncys` key whose value is a collection of
   the NCYs for that school phase, returns a map mapping NCY to school-phase.
   Note that if an NCY is (erronously) specified in multiple school-phases then the
   first defined is used."
  ([] (ncy->school-phase-map dictionary))
  ([dictionary]
   (into (sorted-map)
         (map (fn [[k {:keys [ncys]}]]
                (zipmap ncys (repeat k))))
         (reverse dictionary))))

(defn ncy->school-phase
  "Given National Curriculum Year `x` and [optional] `dictionary` mapping each
   school-phase to a map containing a `:ncys` key whose value is a collection of
   the NCYs for that school phase, returns the school-phase (key) containing it.
   The `dictionary` defaults fo the namespace definition if not specified."
  ([x] (ncy->school-phase x dictionary))
  ([x dictionary]
   (get (ncy->school-phase-map dictionary) x)))