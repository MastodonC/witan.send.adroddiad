(ns witan.send.adroddiad.dsg-plan
  "Definitions and functions for completing the DSG Management Plan 2022-23 (Version 5)."
  (:require [witan.send.adroddiad.ncy :as ncy]
            [com.climate.claypoole.lazy :as lazy]
            [tablecloth.api :as tc]
            [tech.v3.dataset.reductions :as ds-reduce]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary.report :as summary-report]))


(def dsg-category-columns
  "Vector of names of DSG category columns"
  [:dsg-placement-category :age-group :need])


(def friendly-column-names
  "Map keyword column names to friendly names for display"
  {:calendar-year               "Calendar/census year"
   :dsg-placement-category      "DSG placement category abbreviation"
   :dsg-placement-category-name "DSG placement category"
   :age-group                   "Age Group"
   :need                        "ECHP Primary Need Abbreviation"})


;;; Placement categories
(def dsg-placement-category->order
  "Map DSG Management Plan placement category abbreviations to presentation order"
  (let [m (zipmap ["MmSoA" "RPSEN" "MdSSA" "NMISS" "HspAP" "P16FE" "OTHER"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))


(def dsg-placement-categories
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get dsg-placement-category->order k1) k1]
                              [(get dsg-placement-category->order k2) k2]))
         (keys dsg-placement-category->order)))


(def dsg-placement-category->name
  "Map DSG Management Plan placement category abbreviation to corresponding short name"
  (into (sorted-map-by (fn [k1 k2] (compare [(get dsg-placement-category->order k1) k1]
                                            [(get dsg-placement-category->order k2) k2]))) 
        {"MmSoA"	"Mainstream Schools or Academies"
         "RPSEN"	"Resourced Provision or SEN Units"
         "MdSSA"	"Maintained Special Schools or Academies"
         "NMISS"	"Non-Maintained or Independent Special"
         "HspAP"	"Hospital Schools or Alternative Provision"
         "P16FE"	"Post 16 and Further Education"
         "OTHER"	"Other Placements or Direct Payments"}))


;;; Age groups 
(def age-group->order
  "Map DSG Management Plan age groups to presentation order"
  (let [m {"Under 5"      0
           "Age 5 to 10"  5
           "Age 11 to 15" 11
           "Age 16 to 19" 16
           "Age 20 to 25" 20}]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))


(def age-groups
  "DSG Management plan age groups."
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get age-group->order k1) k1]
                              [(get age-group->order k2) k2]))
         (keys age-group->order)))


(defn age-at-start-of-school-year->age-group
  "Age groups for DSG management plan

  From the \"Introduction\" tab of the DSG Management Plan template v5
  and per section 2, part 1, item 1.1 of the [2022 SEN2
  guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf).

  Age is age in whole number of years on 31st August prior to starting the school/academic year."
  [age]
  (cond
    (<=  0 age  4) "Under 5"
    (<=  5 age 10) "Age 5 to 10"
    (<= 11 age 15) "Age 11 to 15"
    (<= 16 age 19) "Age 16 to 19"
    (<= 20 age 25) "Age 20 to 25"
    :else          nil))


(def ncy->age-group
  "Map National Curriculum Year to DSG Management Plan age group."
  (into (sorted-map)
        (map (fn [ncy] [ncy (-> ncy
                                ncy/ncy->age-at-start-of-school-year
                                age-at-start-of-school-year->age-group)])
             ncy/ncys)))


;;; Needs
(def dsg-need-order
  "Map EHCP Primary Need abbreviations to presentation order for DSG Management Plan"
  (let [m (zipmap ["ASD" "HI" "MLD" "MSI" "PD" "PMLD" "SEMH" "SLCN" "SLD" "SPLD" "VI" "OTH" "UKN"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))


(def needs
  "EHCP Primary Need abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get dsg-need-order k1) k1]
                              [(get dsg-need-order k2) k2]))
         (keys dsg-need-order)))


(def dsg-need-names
  "Map EHCP Primary Need abbreviations to names used in sheets of the DSG Management Plan"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get dsg-need-order k1) k1]
                                             [(get dsg-need-order k2) k2])))
        {"ASD"  "Autistic Spectrum Disorder"
         "HI"   "Hearing Impairment"
         "MLD"  "Moderate Learning Difficulty"
         "MSI"  "Multi- Sensory Impairment"
         "PD"   "Physical Disability"
         "PMLD" "Profound & Multiple Learning Difficulty"
         "SEMH" "Social, Emotional and Mental Health"
         "SLCN" "Speech, Language and Communications needs"
         "SLD"  "Severe Learning Difficulty"
         "SPLD" "Specific Learning Difficulty"
         "VI"   "Visual Impairment"
         "OTH"  "Other Difficulty/Disability"
         "UKN"  "SEN support but no specialist assessment of type of need"}))


;;; DSG category combinations
(def dsg-categories-ds
  "Dataset containing the combinations of `:dsg-placement-category`
  `:age-group` and `:need` needed to complete the DSG Management Plan"
  (-> (reduce tc/cross-join (map tc/dataset [[[:dsg-placement-category dsg-placement-categories]]
                                             [[:age-group age-groups]]                       
                                             [[:need needs]]]))
      (tc/drop-rows #(and (= "P16FE" (:dsg-placement-category %))
                          (nil? (#{"Age 16 to 19" "Age 20 to 25"} (:age-group %)))))
      (tc/set-dataset-name "DSG-Categories")))


;;; Functions to assemble transitions (historical and simulated) for analysis
(defn historical-transitions-file->ds
  "Returns a dataset derived from the historical transitions read
  from file `historical-transitions-file` processed into the form of
  the simulated transitions."
  [historical-transitions-file]
  (summary-report/historical-transitions->simulated-counts historical-transitions-file))


(defn simulated-transitions-files->seq
  "Returns a lazy sequence of n datasets of simulated transitions read
  from files (where n is the number of simulations). Each dataset
  contains the transition-counts for a single simulation. It is a lazy
  sequence and can be streamed to ds-reduce."
  [simulated-transitions-files
   & {:keys [cpu-pool]
      :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (sequence cat (lazy/upmap cpu-pool (partial summary-report/read-and-split :simulation) simulated-transitions-files)))


(defn transitions-seq
  "Prepends `historical-transitions-ds` onto each dataset in (lazy)
   sequence `simulated-transitions-seq`.  Note that the values of a
   `:simulation` column in `historical-transitions-ds` are left as-is and
   are not updated to match the value from the corresponding dataset from
   `simulated-transitions-seq`."
  [historical-transitions-ds simulated-transitions-seq]
  (map (fn [ds] (tc/concat historical-transitions-ds ds)) simulated-transitions-seq))


(defn transitions-files->transitions-seq
  "Returns a lazy sequence of n datasets of historical and simulated
  transitions read from files (where n is the number of simulations).
  Each dataset contains the transition-counts for a single simulation
  with the historical transitions prepended. It is a lazy sequence
  and can be streamed to ds-reduce."
  [historical-transitions-file simulated-transitions-files
   & {:keys [cpu-pool]
      :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (let [historical-transitions-ds (historical-transitions-file->ds  historical-transitions-file)
        simulated-transitions-seq (simulated-transitions-files->seq simulated-transitions-files {:cpu-pool cpu-pool})]
    (transitions-seq historical-transitions-ds simulated-transitions-seq)))

;; FIXME: Remove if not used.
(defn transitions-files->transitions-seq-with-separate-history
  "Returns a lazy sequence of n+1 datasets of historical or simulated
  transitions read from files (where n is the number of simulations):
  The first contains the transition-counts for the history, the rest
  contain the transition-counts from a single simulation. It is a lazy
  sequence and can be streamed to ds-reduce."
  [historical-transitions-file simulated-transitions-files
   & {:keys [cpu-pool]
      :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (let [historical-transitions-ds (summary-report/historical-transitions->simulated-counts historical-transitions-file)]
    (as-> simulated-transitions-files $
      (lazy/upmap cpu-pool (partial summary-report/read-and-split :simulation) $)
      (sequence cat $)
      (conj $ historical-transitions-ds))))


;;; Functions to transform and summarise the individual simulation datasets
(defn summarise-census
  "Summarise `census` dataset for DSG plan.
  Completion is not required at this point as only calculating sums. "
  [census & {:keys [domain-keys
                    value-key]
             :or   {domain-keys [:calendar-year :dsg-placement-category :age-group :need]
                    value-key   :transition-count}}]
  (-> census
      (tc/group-by domain-keys)
      (tc/aggregate {value-key #(dfn/sum (value-key %))})))


(defn transform-simulations
  "Apply function specified as `:simulation-transform-f` value to each
  dataset in lazy seq specified as `:simulations-seq` value, returning
  a lazy seq of the processed datasets."
  [simulations-seq simulation-transform-f & {:keys [cpu-pool]
                                             :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (lazy/upmap cpu-pool simulation-transform-f simulations-seq))


;;; Functions to calculate summary stats across simulations
(defn summarise-simulations
  "Summarise column specified as value of `:value-key` over lazy seq of
  simulation datasets `simulations-seq` within groups specified (as
  vector) value of `:domain-keys`. Returns a dataset.  Assumes that
  each datast of `simulations-seq` is a simulation (possibly including
  history but not just history) such that `(count simulations-seq)` is
  the number of simulations." 
  [simulations-seq & {:keys [domain-keys
                             value-key]
                      :or   {domain-keys [:calendar-year :dsg-placement-category :age-group :need]
                             value-key   :transition-count}}]
  (let [sim-count (count simulations-seq)]
    (-> (ds-reduce/group-by-column-agg domain-keys
                                       {:sum       (ds-reduce/sum value-key)
                                        :row-count (ds-reduce/row-count)
                                        ;; TODO: remove.
                                        #_#_:mean-non-missing (ds-reduce/mean value-key)}
                                       simulations-seq)
        (tc/add-column :sim-count sim-count)
        ;; FIXME: remove.
        (tc/map-columns :mean [:sum] #(/ %1 sim-count))
        )))


(defn complete-dsg-category-stats
  "Given dataset of `category-stats` with `:mean` for the
   `:calendar-year` and DSG categories seen in the data, returns dataset
   with :mean for the complete set of DSG categories and
   `:calendar-year`s, with invalid category combinations dropped and rows
   added for any missing DSG category combinations (for any
   `:calendar-year`) with :mean of 0."
  [category-stats]
  (-> (tc/cross-join (tc/dataset [[:calendar-year ((comp sort distinct :calendar-year) category-stats)]])
                     dsg-categories-ds)
      (tc/left-join (-> category-stats
                        (tc/select-columns (conj dsg-category-columns :calendar-year :mean))
                        (tc/set-dataset-name "category-stats"))
                    dsg-category-columns)
      (tc/drop-columns #"^:category-stats\..*")
      (tc/replace-missing :mean :value 0)
      (tc/order-by [#(dsg-placement-category->order (:dsg-placement-category %))
                    #(age-group->order (:age-group %))
                    #(dsg-need-order (:need %))
                    :calendar-year])))

#_(defn summarise-setting-by-age-group [census]
    (-> census
        (tc/map-columns :age-group [:academic-year]
                        (fn [ay]
                          (-> ay
                              ncy/ncy->age-at-start-of-school-year
                              age-at-start-of-school-year->age-group)))
        (tc/group-by [:calendar-year :setting :age-group])
        (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
        (tc/complete :calendar-year :setting :age-group)
        (tc/replace-missing :transition-count :value 0)))


#_(defn summarise-setting-by-need [census]
    (-> census
        (tc/group-by [:calendar-year :setting :need])
        (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
        (tc/complete :calendar-year :setting :need)
        (tc/replace-missing :transition-count :value 0)))



(comment
  
  (defn by-age-wide
    ([summary metric]
     (-> summary
         (tc/select-columns [:calendar-year :setting :age-group metric])
         (tc/order-by [:setting :age-group :calendar-year])
         (tc/pivot->wider [:calendar-year] metric {:drop-missing? false})
         (tc/order-by [:setting :age-group])
         (tc/rename-columns {:setting "Placement"
                             :age-group "Age Group"})))
    ([summary]
     (by-age-wide summary :mean)))

  (defn by-need-wide
    ([summary metric]
     (-> summary
         (tc/select-columns [:calendar-year :setting :need metric])
         (tc/order-by [:setting :need :calendar-year])
         (tc/pivot->wider [:calendar-year] metric {:drop-missing? false})
         (tc/order-by [:setting :need])
         (tc/rename-columns {:setting "Placement"
                             :need "Primary Need"})))
    ([summary]
     (by-need-wide summary :mean)))


  )



(comment

  ;; by-age and by-need examples
  (def cpu-pool (cp/threadpool (- (cp/ncpus) 2)))

  @(def by-age
     (->> {:historical-transitions-file (str in-dir "transitions.csv")
           :simulated-transitions-files pqt-files
           :cpu-pool cpu-pool}
          (simulations-seq)
          (assoc
           {:simulations-transform-f (fn [simulation]
                                       (-> simulation
                                           at/transitions->census
                                           (tc/map-columns :setting [:setting]
                                                           (fn [s]
                                                             (roll-up-names s)))
                                           (tc/drop-rows #(< 20 (:academic-year %)))
                                           summarise-setting-by-age-group))
            :cpu-pool cpu-pool}
           :simulations)
          (transform-simulations)
          (assoc
           {:domain-keys [:calendar-year :setting :age-group]
            :value-key :transition-count}
           :simulations)
          (summarise-simulations)))

  @(def by-need
     (->> {:historical-transitions-file (str in-dir "transitions.csv")
           :simulated-transitions-files pqt-files
           :cpu-pool cpu-pool}
          (simulations-seq)
          (assoc
           {:simulations-transform-f (fn [simulation]
                                       (-> simulation
                                           at/transitions->census
                                           (tc/map-columns :setting [:setting]
                                                           (fn [s]
                                                             (roll-up-names s)))
                                           (tc/drop-rows #(< 20 (:academic-year %)))
                                           summarise-setting-by-need))
            :cpu-pool cpu-pool}
           :simulations)
          (transform-simulations)
          (assoc
           {:domain-keys [:calendar-year :setting :need]
            :value-key :transition-count}
           :simulations)
          (summarise-simulations)))



  )
