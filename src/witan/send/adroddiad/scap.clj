(ns witan.send.adroddiad.scap
  "Definitions and functions for SCAP reporting."
  (:require [clojure.set :as set]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.school-phase :as school-phase]))



;;; # SCAP SEND Provision Types
(defn compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(def scap-send-provision-types
  "SCAP SEND provision types."
  (as-> {"UR" {:order       1,
               :name        "SEN units and resourced provision in mainstream schools"
               :label       "SEN Units & Resourced Provision"
               :definition  "SEN units & resourced provision in mainstream schools."}
         "IN" {:order       2
               :name        "Independent schools (independent schools and independent special schools)"
               :label       "Independent Schools"
               :definition  "Independent schools (independent schools and independent special schools)."}
         "SS" {:order       3
               :name        "State funded special schools (LA-maintained, special academies, non-maintained special schools)"
               :label       "State Funded Special Schools"
               :definition  "State-funded special schools (LA-maintained schools, special academies, non-maintained special schools)."}
         "AP" {:order       4
               :name        "Alternative provision (PRUs, AP academies and independent AP)"
               :label       "Alternative Provision"
               :definition  "Alternative provision (PRUs, AP academies and any other AP)."}} $
    (into (sorted-map-by (partial compare-mapped-keys (update-vals $ :order))) $)))

(def scap-send-provision-types-ds
  "SCAP SEND provision types as a dataset."
  (as-> scap-send-provision-types $
    (map (fn [[k v]] (assoc v :abbreviation k)) $)
    (tc/dataset $)
    (tc/reorder-columns $ [:abbreviation])
    (tc/set-dataset-name $ "scap-send-provision-types")))
