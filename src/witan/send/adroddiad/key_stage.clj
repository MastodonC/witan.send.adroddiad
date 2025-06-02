(ns witan.send.adroddiad.key-stage
  "Definitions and functions for handling Mastodon C's definitions of National Curriculum Key Stage.
   - KS1-4 from https://www.gov.uk/national-curriculum
   - Early Years as 'from birth to 5 years old' per https://www.gov.uk/early-years-foundation-stage
   - KS5 reflecting right of CYP to 3 years post secondary education.
   - Post-19 included to cover SEND age range of 0-25."
  (:require [tablecloth.api :as tc]))



;;; # Utility functions
(defn compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))


;;; # Key Stage
;; Note that in contrast to the key stages previously defined in `witan.send.domain.academic-years`:
;; - "post-19" is used rather than "ncy15+".
;; - Keys are abbreviations rather than keywords, to facilitate use in datasets & files as strings.
;; - Abbreviations are hyphenated so "keyword friendly" and can be converted to keywords if required.
;; - The map is sorted so that `(keys key-stages)` returns the school phases in the correct order.
(def key-stages
  "Mastodon C National Curriculum Key Stage definitions as a sorted map."
  (-> {"early-years" {:order      0
                      :name       "Early Years Foundation Stage"
                      :label      "Early Years"
                      :definition "From birth to 5 years old"
                      :ncy-from   -4 ;; Note `witan.send.domain.academic-years/early-years` includes -5.
                      :ncy-to     0}
       "key-stage-1" {:order      1
                      :name       "Key Stage 1"
                      :label      "KS1"
                      :definition "Age 5 to 7"
                      :ncy-from   1
                      :ncy-to     2}
       "key-stage-2" {:order      2
                      :name       "Key Stage 2"
                      :label      "KS2"
                      :definition "Age 7 to 11"
                      :ncy-from   3
                      :ncy-to     6}
       "key-stage-3" {:order      3
                      :name       "Key Stage 3"
                      :label      "KS3"
                      :definition "Age 11 to 14"
                      :ncy-from   7
                      :ncy-to     9}
       "key-stage-4" {:order      4
                      :name       "Key Stage 4"
                      :label      "KS4"
                      :definition "Age 14 to 16"
                      :ncy-from   10
                      :ncy-to     11}
       "key-stage-5" {:order      5
                      :name       "Key Stage 5"
                      :label      "KS5"
                      :definition "Age 16 to 19"
                      :ncy-from   12
                      :ncy-to     14}
       "post-19"     {;; Note `witan.send.domain.academic-years` had this as `ncy-15+`.
                      :order      6
                      :name       "Post 19"
                      :label      "Post 19"
                      :definition "Post 19"
                      :ncy-from   15
                      :ncy-to     20}}
      (update-vals (fn [m] (assoc m :ncys (into (sorted-set) (range (:ncy-from m) (inc (:ncy-to m)))))))
      (as-> $ (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :order))) $))))

(def key-stages-ds
  "Mastodon C National Curriculum Key Stage definitions as a dataset."
  (as-> key-stages $
    (map (fn [[k v]] (assoc v :abbreviation k)) $)
    (tc/dataset $)
    (tc/reorder-columns $ [:abbreviation])
    (tc/set-dataset-name $ "key-stages")))



;;; # Functions to manipulate key-stages
(defn ncy->key-stage-map
  "Given [optional] `key-stages` definition map (defaults to namespace definition) 
   mapping each key-stage to a map containing a `:ncys` key whose value is a 
   collection of the NCYs for that key stage, returns a map mapping NCY to key-stage.
   Note that if an NCY is (erronously) specified in multiple key-stages then the
   first defined is used."
  ([] (ncy->key-stage-map key-stages))
  ([key-stages]
   (into (sorted-map)
         (map (fn [[k {:keys [ncys]}]]
                (zipmap ncys (repeat k))))
         (reverse key-stages))))

(defn ncy->key-stage
  "Given National Curriculum Year `x` and [optional] `key-stages` definition map
   mapping each key-stage to a map containing a `:ncys` key whose value is a 
   collection of the NCYs for that key stage, returns the key-stage (key) for
   containing it. `key-stages` defaults fo the namespace definition if not specified."
  ([x] (ncy->key-stage x key-stages))
  ([x key-stages]
   (get (ncy->key-stage-map key-stages) x)))
