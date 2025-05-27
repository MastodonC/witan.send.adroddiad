(ns witan.send.adroddiad.scap
  "Definitions and functions for SCAP reporting."
  (:require [clojure.math :as math]
            [clojure.set :as set]
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
;; Note: SCAP *SEND* planning areas (and `scap-send-planning-areas`) is used to refer to the subset
;; of the SCAP Planning Areas (`:scap-planning-area`) used for the SEND demand forecasts:
;; `:scap-planning-area` is used for column names rather than `:scap-send-planning-area`.
(def scap-send-planning-areas-with-school-phase-and-provision-type
  "SCAP SEND planning area codes (as strings) with school-phase and SCAP SEND provision type abbreviations."
  (tc/dataset [["1803001" "primary"   "UR"]
               ["1803002" "primary"   "IN"]
               ["1803003" "primary"   "SS"]
               ["1803004" "primary"   "AP"]
               ["1803011" "secondary" "UR"]
               ["1803012" "secondary" "IN"]
               ["1803013" "secondary" "SS"]
               ["1803014" "secondary" "AP"]]
              {:column-names [:scap-planning-area :school-phase :scap-send-provision-type]
               :dataset-name "scap-send-planning-areas-with-school-phase-and-provision-type"}))

(def scap-send-planning-area->school-phase-and-scap-send-provision-type
  "Map mapping SCAP SEND planning area codes (as strings) to 
   (map of) `:school-phase` and `:scap-send-provision-type`."
  (as-> scap-send-planning-areas-with-school-phase-and-provision-type $
    (zipmap (-> $ :scap-planning-area)
            (-> $
                (tc/drop-columns [:scap-planning-area])
                (tc/rows :as-maps)))
    (into (sorted-map) $)))

(def school-phase-and-scap-send-provision-type->scap-planning-area
  "Map mapping (map of) `:school-phase` and `:scap-send-provision-type` 
   to SCAP planning area code (as string)."
  (set/map-invert scap-send-planning-area->school-phase-and-scap-send-provision-type))

(defn scap-send-planning-areas
  "Returns map mapping SCAP SEND planning area codes to names, labels & definitions.
   Specify `:school-phases` and/or `:scap-send-provision-types` 
   to customise names, labels and definitions."
  [& {:keys [school-phases
             scap-send-provision-types]
      :or   {school-phases             school-phase/school-phases
             scap-send-provision-types scap-send-provision-types}}]
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
        scap-send-planning-area->school-phase-and-scap-send-provision-type))

(defn scap-send-planning-areas->ds
  [scap-send-planning-areas]
  (-> scap-send-planning-areas
      ((partial map (fn [[k v]] (assoc v :scap-planning-area k))))
      tc/dataset
      (tc/order-by [:scap-planning-area])
      (tc/add-column :order (iterate inc 1))
      (as-> $ (tc/reorder-columns $ (concat [:scap-planning-area :order]
                                            (for [prefix [nil "school-phase" "scap-send-provision-type"]
                                                  suffix [nil "name" "label" "definition"]]
                                              (keyword (str prefix (when (and prefix suffix) "-") suffix))))))
      (tc/set-dataset-name "scap-send-planning-areas")))

(defn scap-send-planning-areas-ds
  "Wraps `scap-send-planning-areas` to return as a dataset."
  [& opts]
  (-> (scap-send-planning-areas opts)
      scap-send-planning-areas->ds))



;;; # Functions for summarising simulations for SCAP SEND provision forecasts
(defn scap-send-provision-type-census-transform-fn
  "Given function mapping setting abbreviations to scap-send-provision-type, returns
   census-transform-fn suitable for preparing simulated census for summarisation 
   by [`:scap-send-provision-type`, `:academic-year`, `:calendar-year`] for SCAP."
  [setting->scap-send-provision-type]
  (fn [census-ds]
    (-> census-ds
        (tc/select-rows (comp (into #{} (range 0 12)) :academic-year))
        (tc/map-columns :scap-send-provision-type [:setting] setting->scap-send-provision-type)
        (tc/drop-missing [:scap-send-provision-type]))))

(defn ncy->label
  "Given integer National Curriculum Year, returns string label for SCAP."
  [x]
  (if (zero? x)
    "Reception"
    (format "%d" x)))

(defn census-year->scholastic-year
  "Given integer census year `cy`, returns scholastic year in YYYY/YY string format."
  [cy]
  (format "%s/%s" (dec cy) (rem cy 100)))

(defn scap-send-provision-type-count-summary->scap-send-demand-forecast
  "Given summary of projected counts per `:scap-send-provision-type` per 
   `:academic-year` per `:calendar-year`, adds `:scap-planning-area` and
   formats the dataset for SCAP submission."
  [scap-send-provision-type-count-summary]
  (-> scap-send-provision-type-count-summary
      ;; Derive school phase and SCAP special planning area codes
      (tc/map-columns :school-phase [:academic-year] school-phase/ncy->school-phase)
      (tc/map-rows (fn [r]
                     {:scap-planning-area
                      (get school-phase-and-scap-send-provision-type->scap-planning-area
                           (select-keys r [:school-phase :scap-send-provision-type]))}))
      ;; Rename analysis `:academic-year` to `:ncy` and derive NCY name as required for SCAP
      (tc/rename-columns {:academic-year :ncy})
      (tc/map-columns :ncy-label [:ncy] ncy->label)
      ;; Rename analysis `:calendar-year` to `:census-year` (to avoid confusion) 
      ;; and derive academic year as required for SCAP
      (tc/rename-columns {:calendar-year :census-year})
      (tc/map-columns :scholastic-year [:census-year] census-year->scholastic-year)
      ;; Add rounded median and rounded mean (for reporting whole numbers of CYP), rounding x.5 up.
      (tc/map-columns :rounded-median [:median] math/round)
      (tc/map-columns :rounded-mean   [:mean]   math/round)
      ;; Arrange dataset
      (tc/order-by [:scap-planning-area-code
                    :census-year
                    :ncy])
      (tc/reorder-columns [:school-phase
                           :scap-send-provision-type
                           :scap-planning-area
                           :census-year
                           :scholastic-year
                           :ncy
                           :ncy-label
                           :simulation-count
                           :observations
                           :min
                           :p05
                           :q1
                           :median
                           :q3
                           :p95
                           :max
                           :mean
                           :rounded-median
                           :rounded-mean])
      (tc/set-dataset-name "scap-send-demand-forecasts")))

(def scap-send-demand-forecast-ds-col-name->label
  {:school-phase             "School Phase"
   :scap-send-provision-type "Provision Type"
   :scap-planning-area       "Planning Area"
   :census-year              "SEN2 Census Year"
   :scholastic-year          "Scholastic Year"
   :ncy                      "NCY"
   :ncy-label                "NCY label"
   :simulation-count         "№ simulations"
   :observations             "№ observations"
   :min                      "Min. of simulations"
   :p05                      "5th %-ile of simulations"
   :q1                       "25th %-ile of simulations"
   :median                   "Median of simulations"
   :q3                       "75th %-ile of simulations"
   :p95                      "95th %-ile of simulations"
   :max                      "Max. of simulations"
   :mean                     "Mean of simulations"
   :rounded-median           "Meidan of simulations (rounded)"
   :rounded-mean             "Mean of simulations (rounded)"})



;;; # Functions to assist writing SCAP SEND demand forecasts to files
(defn select-and-pivot->scap-planning-area-table
  "Given dataset of SCAP SEND demand forecasts `scap-send-demand-forecasts`,
   filters for specified `scap-planning-area` and pivots wide into the form
   of the SCAP planning area tables in the submission template."
  [scap-send-demand-forecasts scap-planning-area]
  (-> scap-send-demand-forecasts
      (tc/select-rows #(-> % :scap-planning-area (= scap-planning-area)))
      (tc/select-columns [:scap-planning-area :scholastic-year :census-year :ncy-label :rounded-median])
      (tc/pivot->wider :ncy-label :rounded-median {:drop-missing? false})))

(defn scap-send-demand-forecast-ds->scap-return-sheet-specs
  "Given `scap-send-demand-forecasts` dataset and `seed-year`, 
   returns sequence of `kixi.large` sheet specifications 
   with the SCAP SEND demand forecasts for each SCAP SEND planning area."
  [scap-send-demand-forecasts seed-year]
  (for [scap-planning-area (keys (scap-send-planning-areas))]
    (let [school-phase      (-> scap-planning-area
                                scap-send-planning-area->school-phase-and-scap-send-provision-type
                                :school-phase)
          census-year-range (into (sorted-set)
                                  (map #(+ seed-year %))
                                  (cond
                                    (= "primary"   school-phase) (range 1 (inc 5))
                                    (= "secondary" school-phase) (range 1 (inc 7))))]
      #:kixi.large{:sheet-name scap-planning-area
                   :data       (-> scap-send-demand-forecasts
                                   (select-and-pivot->scap-planning-area-table scap-planning-area)
                                   (tc/select-rows (comp census-year-range :census-year))
                                   (tc/drop-columns [:scap-planning-area :census-year])
                                   (tc/rename-columns {:scholastic-year "Forecasts"}))})))
