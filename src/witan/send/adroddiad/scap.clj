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



;;; # SCAP SEND Planning Areas
(def scap-send-planning-area-codes-ds
  "SCAP SEND planning area codes (as strings) with school-phase and SCAP SEND provision type abbreviations."
  (tc/dataset [["1803001" "primary"   "UR"]
               ["1803002" "primary"   "IN"]
               ["1803003" "primary"   "SS"]
               ["1803004" "primary"   "AP"]
               ["1803011" "secondary" "UR"]
               ["1803012" "secondary" "IN"]
               ["1803013" "secondary" "SS"]
               ["1803014" "secondary" "AP"]]
              {:column-names [:code :school-phase :scap-send-provision-type]
               :dataset-name "scap-send-planning-area-codes"}))

(def scap-send-planning-area-codes
  "Map mapping SCAP SEND planning area codes (as strings) to 
   (map of) `:school-phase` and `:scap-send-provision-type`."
  (as-> scap-send-planning-area-codes-ds $
    (zipmap (-> $ :code)
            (-> $
                (tc/drop-columns [:code])
                (tc/rows :as-maps)))
    (into (sorted-map) $)))

(def school-phase-and-scap-send-provision-type->scap-send-planning-area
  "Map mapping (map of) `:school-phase` and `:scap-send-provision-type` 
   to SCAP SEND planning area code (as string)."
  (set/map-invert scap-send-planning-area-codes))

(defn scap-send-planning-areas
  "Returns `scap-send-planning-area-codes` map extended to map SCAP SEND 
   planning area codes to names, labels & definitions.
   Specify `:school-phases` and/or `:scap-send-provision-types` 
   to customise names, labels and definitions."
  [& {:keys [scap-send-planning-area-codes
             school-phases
             scap-send-provision-types]
      :or   {scap-send-planning-area-codes scap-send-planning-area-codes
             school-phases                 school-phase/school-phases
             scap-send-provision-types     scap-send-provision-types}}]
  (into (sorted-map)
        (map (fn [[k v]]
               (let [school-phase              (get v :school-phase)
                     school-phase'             (get school-phases
                                                    school-phase
                                                    {:name school-phase})
                     scap-send-provision-type  (get v :scap-send-provision-type)
                     scap-send-provision-type' (get scap-send-provision-types
                                                    scap-send-provision-type
                                                    {:name scap-send-provision-type})]
                 [k (merge v
                           {:name       (format "%s Years %s" (:name       school-phase') (:name       scap-send-provision-type'))
                            :label      (format "%s %s"       (:label      school-phase') (:label      scap-send-provision-type'))
                            :definition (format "%s years %s" (:definition school-phase') (:definition scap-send-provision-type'))}
                           (-> school-phase'
                               (select-keys [:name :label :abbreviation :definition])
                               (update-keys #(->> % name (str "school-phase-") keyword)))
                           (-> scap-send-provision-type'
                               (select-keys [:name :label :definition :abbreviation])
                               (update-keys #(->> % name (str "scap-send-provision-type-") keyword))))])))
        scap-send-planning-area-codes))

(defn scap-send-planning-areas-ds
  "Wraps `scap-send-planning-area-codes` to return as a dataset."
  [& opts]
  (-> (scap-send-planning-areas opts)
      ((partial map (fn [[k v]] (assoc v :code k))))
      tc/dataset
      (tc/order-by [:code])
      (tc/add-column :order (iterate inc 1))
      (as-> $ (tc/reorder-columns $ (concat [:code :order]
                                            (for [prefix [nil "school-phase" "scap-send-provision-type"]
                                                  suffix [nil "name" "label" "definition"]]
                                              (keyword (str prefix (when (and prefix suffix) "-") suffix))))))
      (tc/set-dataset-name "scap-send-planning-areas")))


