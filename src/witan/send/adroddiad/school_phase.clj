(ns witan.send.adroddiad.school-phase
  "Definitions and functions for handling Mastodon C's definitions of school phase for SEND reporting and planning:
   Note that:
   - early-childhood is everything before starting school in reception
   - primary         includes reception
   - secondary       ends at NCY 11 (age 15-16), and excludes sixth-form
   - education after secondary is split into post-16 and post-19,
     where the ages 16 & 19 indicated are the nominal ages for the NCYs included†, and are inclusive:
     - post-16: NCYs 12–14 (corresponding to ages 16–18 at start of NCY†)
     - post-19: NCYs 15–20 (corresponding to ages 18–24 at start of NCY†)
     …with post-16 being 3 years (rather than the 2 of a sixth-form)
      matching the right of CYP to 3 years of post secondary education.

   † Where ages are given, these relate to the nominal NCY for the age on 31st August prior to the start of the school year.
     (For details and mapping of age to nominal NCY, see `witan.send.adroddiad.ncy`.)"
  (:require [tablecloth.api :as tc]))



;;; # Utility functions
(defn compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))


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
;; - The map is sorted so that `(keys school-phases)` returns the school phases in the correct order.

(def school-phases
  "Mastodon C School Phase definitions as a sorted map."
  (as-> {"early-childhood" {:order      0
                            :name       "Early Childhood Education and Care"
                            :label      "Early Childhood"
                            :definition "Prior to starting school: Age 0-4"
                            :ncy-from   -4 ;; Note witan.send.domain.academic-years/nursery includes -5
                            :ncy-to     -1}
         "primary"         {:order      1
                            :name       "Primary"
                            :label      "Primary"
                            :definition "Primary school: Reception, KS1 & KS2 - Reception + NCYs 1 to 6"
                            :ncy-from   0
                            :ncy-to     6}
         "secondary"       {:order      2
                            :name       "Secondary"
                            :label      "Secondary"
                            :definition "Secondary school: Key Stages 3 + 4 - NCYs 7 to 11"
                            :ncy-from   7
                            :ncy-to     11}
         "post-16"         {:order      3
                            :name       "Post 16"
                            :label      "Post 16"
                            :definition "Post 16 - Key Stage 5 - NCYs 12 to 14"
                            :ncy-from   12
                            :ncy-to     14}
         "post-19"         {:order      4
                            :name       "Post 19"
                            :label      "Post 19"
                            :definition "Post 19 - Post Key Stage 5 up to 25 years of age - NCYs 15 to 20"
                            :ncy-from   15
                            :ncy-to     20}} $
    (update-vals $ (fn [m] (assoc m :ncys (into (sorted-set) (range (:ncy-from m) (inc (:ncy-to m)))))))
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :order))) $)))

(def school-phases-ds
  "Mastodon C School Phase definitions as a dataset."
  (as-> school-phases $
    (map (fn [[k v]] (assoc v :abbreviation k)) $)
    (tc/dataset $)
    (tc/reorder-columns $ [:abbreviation])
    (tc/set-dataset-name $ "school-phases")))



;;; # Functions to manipulate school-phases
(defn ncy->school-phase
  "Given National Curriculum Year `x` and [optional] map of `school-phases`,
   returns the abbreviation for the MC School Phase containing it.
  `school-phases` must be a map with keys the school phase
   and values maps containing a `:ncys` key whose value is a collection of the NCYs for that school phase.
   Defaults to the namespace `school-phase` if not specified."
  ([x] (ncy->school-phase x school-phases))
  ([x school-phases]
   (some (fn [[k {:keys [ncys]}]]
           (when (contains? ncys x) k))
         school-phases)))

