(ns witan.send.adroddiad.dsg-plan
  "Definitions and functions for completing the DSG Management Plan 2022-23 (Version 5)."
  (:require [clojure.math :as math]
            [com.climate.claypoole.lazy :as lazy]
            [tablecloth.api :as tc]
            [tech.v3.dataset.reductions :as ds-reduce]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.ncy :as ncy]
            [witan.send.adroddiad.summary.report :as summary-report]
            [witan.send.adroddiad.transitions :as at]))


;;; # dsg-plan dataset columns, labels and descriptions
(def dsg-category-columns
  "Vector of names of DSG category columns"
  [:dsg-placement-category :age-group :need])

(def column-names->labels
  "Map keyword column names to labels for display"
  {:composite-key                      "Composite key"
   :dsg-placement-category             "DSG placement category abbreviation"
   :dsg-placement-category-label       "DSG placement category"
   :dsg-placement-category-sheet-title "DSG Management Plan template placement category sheet title"
   :breakdown                          "Breakdown abbreviation"
   :breakdown-label                    "Breakdown"
   :age-group                          "Age group abbreviation"
   :age-group-label                    "Age group"
   :need                               "ECHP primary need abbreviation"
   :need-label                         "ECHP primary need"
   :calendar-year                      "Calendar/census year"
   :mean                               "Estimated number of EHCPs (mean)"
   :rounded-mean                       "Estimated number of EHCPs (rounded mean)"})

(def column-names->descriptions
  "Map keyword column names to descriptions"
  {:composite-key                      (str "Composite key formed as concatenation of "
                                            "`dsg-placement-category-sheet-title`, `breakdown-label`, "
                                            "`age-group-label` or `need-label` (depending on which "
                                            "breakdown the row is for) and `calendar-year` "
                                            "(separated by ': ') to facilitate use of single column "
                                            "lookup functions in spreadsheet applications.")
   :dsg-placement-category             "Mastodon C abbreviation for the DSG placement category."
   :dsg-placement-category-label       "Short label for the DSG placement category."
   :dsg-placement-category-sheet-title "DSG Management Plan template sheet title for this placement category."
   :breakdown                          "Indicates whether the row is for the 'age-group' or 'need' breakdown."
   :breakdown-label                    "Title of breakdown sub-table in the DSG Management Plan template."
   :age-group                          (str "Age group abbreviation, indicating the age group summarised in this row, "
                                            "with 'TOTAL' for the row containing the total over all age groups. "
                                            "Only populated for rows in the age-group breakdown.")
   :age-group-label                    (str "Age group (and total), as labelled in the DSG Management Plan template. "
                                            "Only populated for rows in the age-group breakdown.")
   :need                               (str "ECHP Primary Need abbreviation, indicating the Need summarised in this row, "
                                            "with 'TOTAL' for the row containing the total over all needs. "
                                            "Only populated for rows in the need breakdown.")
   :need-label                         (str "ECHP Primary Need (and total) as labelled in the DSG Management Plan template. "
                                            "Only populated for rows in the need breakdown.")
   :calendar-year                      (str "Calendar/census year: Projections are from SEN2 returns "
                                            "which are made in January, so year YYYY corresponds to "
                                            "January of academic-year YYYY-1 to YYYY.")
   :mean                               "Estimated number of EHCPs (mean from simulations)"
   :rounded-mean                       "Estimated (whole) number of EHCPs (rounded mean from simulations)"})


;;; # Category definitions
;;; ## Placement categories
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

(def dsg-placement-category->label
  "Map DSG Management Plan placement category abbreviation to corresponding short label"
  (into (sorted-map-by (fn [k1 k2] (compare [(get dsg-placement-category->order k1) k1]
                                            [(get dsg-placement-category->order k2) k2]))) 
        {"MmSoA"	"Mainstream Schools or Academies"
         "RPSEN"	"Resourced Provision or SEN Units"
         "MdSSA"	"Maintained Special Schools or Academies"
         "NMISS"	"Non-Maintained or Independent Special"
         "HspAP"	"Hospital Schools or Alternative Provision"
         "P16FE"	"Post 16 and Further Education"
         "OTHER"	"Other Placements or Direct Payments"}))

(def dsg-placement-category->sheet-title
  "Map DSG Management Plan placement category abbreviation to corresponding sheet title"
  (into (sorted-map-by (fn [k1 k2] (compare [(get dsg-placement-category->order k1) k1]
                                            [(get dsg-placement-category->order k2) k2]))) 
        {"MmSoA"	"Mainstream schools or academies placements"
         "RPSEN"	"Resourced provision or SEN Units placements"
         "MdSSA"	"Maintained special schools or special academies placements"
         "NMISS"	"Non-maintained special schools or independent (NMSS or independent) placements"
         "HspAP"	"Hospital schools or alternative provision (AP) placements"
         "P16FE"	"Post 16 and further education (FE) placements"
         "OTHER"	"Other placements or direct payments"}))

;;; ## Age groups
(def age-group->order
  "Map DSG Management Plan age group abbreviations to presentation order"
  (let [m {"Under 5"      0
           "Age 5 to 10"  5
           "Age 11 to 15" 11
           "Age 16 to 19" 16
           "Age 20 to 25" 20}]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def age-groups
  "DSG Management Plan age group abbreviations."
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get age-group->order k1) k1]
                              [(get age-group->order k2) k2]))
         (keys age-group->order)))

(def age-group->label
  "Map DSG Management Plan age group abbreviation to corresponding name"
  (into (sorted-map-by (fn [k1 k2] (compare [(get age-group->order k1) k1]
                                            [(get age-group->order k2) k2])))
        {"Under 5"      "Under 5"
         "Age 5 to 10"  "Age 5 to 10"
         "Age 11 to 15" "Age 11 to 15"
         "Age 16 to 19" "Age 16 to 19"
         "Age 20 to 25" "Age 20 to 25"}))

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


;;; ## Needs
(def need->order
  "Map EHCP Primary Need abbreviations to presentation order for DSG Management Plan"
  (let [m (zipmap ["ASD" "HI" "MLD" "MSI" "PD" "PMLD" "SEMH" "SLCN" "SLD" "SPLD" "VI" "OTH" "UKN"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def needs
  "EHCP Primary Need abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get need->order k1) k1]
                              [(get need->order k2) k2]))
         (keys need->order)))

(def need->label
  "Map EHCP Primary Need abbreviations to labels used in sheets of the DSG Management Plan"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get need->order k1) k1]
                                             [(get need->order k2) k2])))
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


;;; ## DSG category combinations
(def dsg-categories-ds
  "Dataset containing the combinations of `:dsg-placement-category`
  `:age-group` and `:need` needed to complete the DSG Management Plan"
  (-> (reduce tc/cross-join (map tc/dataset [[[:dsg-placement-category dsg-placement-categories]]
                                             [[:age-group age-groups]]                       
                                             [[:need needs]]]))
      (tc/drop-rows #(and (= "P16FE" (:dsg-placement-category %))
                          (nil? (#{"Age 16 to 19" "Age 20 to 25"} (:age-group %)))))
      (tc/set-dataset-name "DSG-Categories")))


;;; # Functions to assemble transitions (historical and simulated) for analysis
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
#_(defn transitions-files->transitions-seq-with-separate-history
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


;;; # Functions to transform and summarise the individual simulation datasets
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

(defn simulation-transform-f
  "Transform function applied to `ds` of transitions (with
  `:transition-counts`) for a single simulation.

  Must return a dataset with a single row per
  [`:calendar-year` `:dsg-placement-category` `:age-group` `:need`].

  Supplied `census-transform-f` function is used to process the
  transitions after conversion to a census dataset to have the
  required `:calendar-year`s, `:dsg-placement-category`s,
  `:age-group`s, and `:need`s."
  [census-transform-f ds]
  (-> ds
      at/transitions->census
      (census-transform-f)
      (tc/map-columns :age-group [:academic-year] #(ncy->age-group %))
      summarise-census)) 

(defn transform-simulations
  "Apply function specified as `simulation-transform` to each
  dataset in lazy seq specified as `simulations-seq` value, returning
  a lazy seq of the processed datasets."
  [simulations-seq simulation-transform & {:keys [cpu-pool]
                                           :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (lazy/upmap cpu-pool simulation-transform simulations-seq))


;;; # Functions to calculate summary stats across simulations
(defn summarise-simulations
  "Summarise column specified as value of `value-key` over lazy seq of
  simulation datasets `simulations-seq` within groups specified (as
  vector) value of `domain-keys`.

  Assumes that each datast of `simulations-seq` is a
  simulation (possibly including history but not just history) such
  that `(count simulations-seq)` is the number of simulations.

  Returns a dataset with one record for each set of `domain-keys` seen
  in the data (no completion), with summary stats columns:
  - `:sum`    - sum of observed `value-key`
  - `:n-obs`  - number of sims with a record for current `domain-keys`
  - `:n-sims` - number of simulations (also added to dataset metadata)
  - `:mean`   - calculated as `:sum`/`:n-sims`

  Implicit in this approach to calculating the `:mean` is the
  assumption that the (- `:n-sims` `:n-obs`) simulations without a
  record have 0 `value-key`." 
  [simulations-seq & {:keys [domain-keys
                             value-key]
                      :or   {domain-keys [:calendar-year :dsg-placement-category :age-group :need]
                             value-key   :transition-count}}]
  (let [n-sims (count simulations-seq)]
    (-> (ds-reduce/group-by-column-agg domain-keys
                                       {:sum   (ds-reduce/sum value-key) ; Sum of observed `value-key`s.
                                        :n-obs (ds-reduce/row-count) ; Number of sims with a record to observe.
                                        ;; Uncomment next line to include mean across sims with observed `value-key`.
                                        ;; :mean-obs (ds-reduce/mean value-key)
                                        }
                                       simulations-seq)
        (tc/add-column :n-sims n-sims)
        (tc/map-columns :mean [:sum] #(/ %1 n-sims)) ; Assumes that `value-key`s not observed would be 0.
        (vary-meta assoc :n-sims n-sims))))

(defn transitions-files->category-stats
  "Given `historical-transitions-file` `simulated-transitions-files`,
  prepends history onto to each simulation, applies the
  `simulation-transform-f` to each and then summarises across
  simulations for each combination of [`:calendar-year`
  `:dsg-placement-category` `:age-group` `:need`] present in the
  historical/simulated data."
  [historical-transitions-file simulated-transitions-files simulation-transform-f
   & {:keys [cpu-pool]
      :or   {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (-> (transitions-files->transitions-seq historical-transitions-file
                                          simulated-transitions-files
                                          {:cpu-pool cpu-pool})
      (transform-simulations simulation-transform-f {:cpu-pool cpu-pool})
      (summarise-simulations)
      (tc/set-dataset-name "category-stats")))

(defn category-stats->complete-dsg-category-stats
  "Given dataset of `category-stats` with metadata including `:n-sims`
  and stats [`:sum` `:n-obs` `:n-sims` `:mean`] for the
  `:calendar-year` and DSG categories seen in the data, returns
  dataset with stats for the complete set of DSG categories and
  `:calendar-year`s, with any non-DSG category combinations dropped and
  rows added for any missing DSG category combinations (for any
  `:calendar-year`) with [`:sum` `:n-obs` `:mean`] of 0 and `:n-sims`
  set from metadata."
  [category-stats]
  (let [n-sims (-> category-stats meta :n-sims)]
    (-> (tc/cross-join (tc/dataset [[:calendar-year ((comp sort distinct :calendar-year) category-stats)]])
                       dsg-categories-ds)
        (tc/left-join (-> category-stats
                          (tc/select-columns (conj dsg-category-columns :calendar-year :sum :n-obs :n-sims :mean))
                          (tc/set-dataset-name "category-stats"))
                      (conj dsg-category-columns :calendar-year))
        (tc/drop-columns #"^:category-stats\..*")
        (tc/replace-missing [:sum :n-obs :mean] :value 0)
        (tc/replace-missing :n-sims :value n-sims)
        (vary-meta assoc :n-sims n-sims)
        (tc/order-by [#(dsg-placement-category->order (:dsg-placement-category %))
                      #(age-group->order (:age-group %))
                      #(need->order      (:need      %))
                      :calendar-year])
        (tc/set-dataset-name "complete-dsg-category-stats"))))


;;; # Functions to roll-up the complete-dsg-category-stats to the age-group and need breakdowns of the dsg-plan

;;; ## Proof that _the mean of the sum is the sum of the means_, when all are over a common denominator.
;; (Using LaTeX mathematical notation.)
;;
;; Consider two categories X & Y with \# CYP $x_i$ & $y_i$ where
;; $i=\{1,\ldots,n\}$ indexes the simulation
;; and $n$ is the number of simulations.
;; The mean \# CYP for X is $\bar{x}_\bullet = \frac{1}{n}\sum_{i=1}^{n} x_i$
;; and for Y is $\bar{y}_\bullet = \frac{1}{n}\sum_{i=1}^{n} y_i$ .
;; Now consider roll-up category Z consisting of X and Y together,
;; with \# CYP $z_i = x_i + y_i$
;; and mean $\frac{1}{n}\bar{z}_\bullet = \sum_{i=1}^{n} z_i$ .
;; Substituting for $z_i$ we have:
;; 
;; $$
;; \bar{z}_\bullet
;; = \frac{1}{n}\sum_{i=1}^{n} z_i
;; = \sum_{i=1}^{n} \frac{z_i}{n}
;; = \sum_{i=1}^{n} \frac{\left(x_i+y_i\right)}{n} \\
;; = \sum_{i=1}^{n} \left( \frac{x_i}{n} + \frac{y_i}{n} \right) \\
;; = \left( \sum_{i=1}^{n} \frac{x_i}{n}  \right) + \left( \sum_{i=1}^{n} \frac{y_i}{n}  \right) \\
;; = \left( \frac{1}{n}\sum_{i=1}^{n} x_i \right) + \left( \frac{1}{n}\sum_{i=1}^{n} y_i \right)
;; = \left( \bar{x}_\bullet               \right) + \left( \bar{y}_\bullet               \right)
;; = \bar{x}_\bullet + \bar{y}_\bullet
;; $$
;; 
;; I.e. $\bar{z}_\bullet = \bar{x}_\bullet + \bar{y}_\bullet$
;; The proof above only relies on the $n$ being the same for X & Y,
;; and can be extended to combinations of any number of
;; non-overlapping categories.
;; 
;; I.e. _the mean of the sum is the sum of the means_,
;; when all are over the same denominator.


;;; ## Rounding
;; 
;; Most readers of the DSG Management Plan will expect the "Number of
;; EHCPs" to be a whole number, even for the "estimated future
;; projections". Therefore we need to consider rounding.
;; 
;; There are three options:
;; 
;; 1. Calculate the by-age-group breakdown, the by-need breakdowns and
;;    the marginal total for each `:calendar-year` directly as the mean of
;;    the corresponding breakdown/total over the simulations and round to
;;    the nearest whole number.
;;    - This gives marginal totals by `:calendar-year` for the
;;      by-age-group and by-need summaries that match up, BUT
;;    - The rounded numbers for a breakdown by-age-group or by-need may not add up to the
;;      rounded total presented for the `:calendar-year` (even though the
;;      underlying unrounded means do add up).
;; 
;; 2. Calculate the by-age-group and by-need breakdowns for each
;;    `:calendar-year` directly as the mean of
;;    the corresponding breakdown over the simulations and round to the
;;    nearest whole number, but calculate the marginal `:calendar-year`
;;    totals for the by-age-group and by-need breakdowns separately as
;;    the sum of the rounded numbers in the breakdown.
;;    - This will result in the rounded figures presented in each
;;      breakdown for a given `:calendar-year` adding up to the marginal
;;      `:calendar-year` total given for that breakdown, BUT
;;    - The marginal `:calendar-year` totals for the by-age-group table
;;      may differ from those calculated for the by-need summary.
;; 
;; 3. Calculate the mean number of EHCPs for each of the
;;    DSG Management Plan category combinations {`:calendar-year` x
;;    `:dsg-placement-category` x `:age-group` x `:need`}, round those
;;    and then calculate the by-age-group and by-need breakdowns and
;;    their marginal totals as roll-ups of those.
;;    - This will result in marginal by `:calendar-year` totals for the
;;      by-age-group-breakdown and by-need summaries that match up and
;;      which are the sum of the figures given in each breakdown, BUT
;;    - The overall total number of EHCPs calculated from these rounded
;;      values _will_ differ to the overall mean calciulated directly
;;      from the simulations (without rounding). Given the skew
;;      distribution of the simulated numbers of EHCPs this approach
;;      will likely under-estimate the number of EHCPs slightly.
;; 
;; Since option 3 under-estimates the total number of EHCPs and option 2
;; can result in differences in the by `:calendar-year` marginal totals,
;; we proceed with option 1. If the resulting rounded figures for a
;; breakdown do not add up to the rounded `:calendar-year` total
;; presented, then we suggest including a note that the whole numbers
;; presented are rounded versions of underlying rational numbers which do
;; add up.  (An alternative would be to manually adjust the rounding of
;; one or more of the values in the breakdown to make it add up, ideally
;; considering rounding (unrounded) values close to .5 the other way.)

(comment
  ;; Assess impact on overall number of EHCPs of using rounded DSG category means.
  (-> complete-dsg-category-stats
      (tc/group-by [:calendar-year])
      (tc/aggregate {:mean             #(reduce + (                :mean %))
                     :sum-rounded-mean #(reduce + (map math/round (:mean %)))})
      (tc/map-columns :difference [:mean :sum-rounded-mean]  #(- %1 %2))
      (tc/reorder-columns [:calendar-year :mean :sum-rounded-mean :difference])
      (tc/rename-columns (merge column-names->labels
                                {:mean             "Total number of EHCPs (from unrounded means)"
                                 :sum-rounded-mean "Total number of EHCPs (from rounded category means)"
                                 :difference       "Difference"})))

  ;; Assess impact on overall number of EHCPs of summing category `:mean`s
  ;; vs. summing the `:sum`s and dividing by `:n-sims` due to propagation
  ;; of rounding errors in binary representation of category `:mean`s.
  (let [n-sims (-> complete-dsg-category-stats meta :n-sims)]
    (-> complete-dsg-category-stats
        (tc/group-by [:calendar-year])
        (tc/aggregate {:mean #(reduce + (:mean %))
                       :sum  #(reduce + (:sum  %))})
        (tc/map-columns :mean-from-sums [:sum] #(/ % n-sims))
        (tc/map-columns :difference [:mean :mean-from-sums]  #(- %1 %2))
        (tc/reorder-columns [:calendar-year :sum :mean :mean-from-sums :difference])
        (tc/rename-columns (merge column-names->labels
                                  {:mean           "Total number of EHCPs"
                                   :mean-from-sums "Total number of EHCPs"
                                   :difference     "Difference"}))))
  )

(defn complete-dsg-category-stats->dsg-plan
  "Given dataset of `complete-dsg-category-stats`, returns a dataset of the
  by-age-group and by-need breakdowns (with totals) required to complete the
  DSG Management Plan.

   Input dataset `complete-dsg-category-stats` must have (only)
  records for each `:dsg-placement-category` `:age-group` `:need`
  combination required for the DSG Management Plan for each
  `:calendar-year`, with the corresponding `:rounded-mean`.

   Returned dataset has columns:
   - `:composite-key` - Composite key to facilitate use of single column lookup functions in spreadsheet applications.
   - `:dsg-placement-category`
   - `:dsg-placement-category-label`
   - `:dsg-placement-category-sheet-title`
   - `:breakdown` - indicating whether the row is for the by age-group or by need breakdown.
   - `:breakdown-label` - description of the breakdown matching that in the DSG Management Plan template.
   - `:age-group`
   - `:age-group-label`
   - `:need`
   - `:need-label`
   - `:calendar-year`
   - `:mean` - Estimated (rational) number of EHCPs, calculated as mean across simulations.
   - `:rounded-mean` - Estimated (whole) number of EHCPs, calculated by rounding `:mean`.

   As all the `:mean`s are calculated over same denominator (number of
   simulations), and as all the categories are non-overlapping, we can
   calculate the `:mean`s for combinations (roll-ups) of categories by
   summing the category `:mean`s, and will get the same answer as if we
   rolled up the `:transition-counts` in each simulation and then
   calculated the mean."
  [complete-dsg-category-stats]
  (let [aggregate-sum          (fn [ds col] (tc/aggregate ds {col #(reduce + (% col))}))
        age-group-total-label  "Total number by Age Group"
        need-total-label       "Total number of EHCPs by primary need"
        by-age-group-breakdown (-> complete-dsg-category-stats
                                   (tc/group-by [:dsg-placement-category :calendar-year :age-group])
                                   (aggregate-sum :mean))
        by-age-group-totals    (-> by-age-group-breakdown
                                   (tc/group-by [:dsg-placement-category :calendar-year])
                                   (aggregate-sum :mean)
                                   (tc/add-column :age-group "TOTAL"))
        by-age                 (-> (tc/concat by-age-group-breakdown by-age-group-totals)
                                   (tc/add-column :breakdown :age-group)
                                   (tc/add-column :breakdown-label "Number of EHCPs by age group"))
        by-need-breakdown      (-> complete-dsg-category-stats
                                   (tc/group-by [:dsg-placement-category :calendar-year :need])
                                   (aggregate-sum :mean))
        by-need-totals         (-> by-need-breakdown ; Should be the same as by-age-group-totals
                                   (tc/group-by [:dsg-placement-category :calendar-year])
                                   (aggregate-sum :mean)
                                   (tc/add-column :need "TOTAL"))
        by-need                (-> (tc/concat by-need-breakdown by-need-totals)
                                   (tc/add-column :breakdown :need)
                                   (tc/add-column :breakdown-label "Number of EHCPs by primary need"))]
    (-> (tc/concat by-age by-need)
        (tc/map-columns :rounded-mean [:mean] #(math/round %))
        (tc/map-columns :dsg-placement-category-label       [:dsg-placement-category] #(dsg-placement-category->label %))
        (tc/map-columns :dsg-placement-category-sheet-title [:dsg-placement-category] #(dsg-placement-category->sheet-title %))
        (tc/map-columns :age-group-label [:age-group] #((assoc age-group->label "TOTAL" age-group-total-label) % %))
        (tc/map-columns :need-label      [:need     ] #((assoc need->label      "TOTAL" need-total-label     ) % %))
        (tc/map-columns :composite-key
                        [:dsg-placement-category-sheet-title :breakdown-label :age-group-label :need-label :calendar-year]
                        (fn [dsg-placement-category-sheet-title breakdown age-group-label need-label calendar-year]
                          (reduce #(str %1 ": " %2) [dsg-placement-category-sheet-title
                                                     breakdown
                                                     (or age-group-label need-label)
                                                     calendar-year])))
        (tc/order-by [#(dsg-placement-category->order (:dsg-placement-category %))
                      :breakdown
                      #((assoc age-group->order "TOTAL" 99) (:age-group %))
                      #((assoc need->order      "TOTAL" 99) (:need      %))
                      :calendar-year])
        (tc/reorder-columns [:composite-key
                             :dsg-placement-category :dsg-placement-category-label :dsg-placement-category-sheet-title
                             :breakdown :breakdown-label
                             :age-group :age-group-label
                             :need :need-label
                             :calendar-year
                             :mean :rounded-mean]))))


;;; # Functions to manipulate the dsg-plan dataset for display
(defn extract-dsg-plan-breakdown
  "Extract DSG Management Plan table from `dsg-plan` dataset for
   specified `dsg-placement-category` & `breakdown` (either `:age-group`
   or `:need`) to be presented wide by `:calendar-year` using labels from
   `label-column` and values from column `value-key`."
  [dsg-plan dsg-placement-category breakdown value-key label-column]
  (-> dsg-plan
      (tc/select-rows #(= dsg-placement-category (:dsg-placement-category %)))
      (tc/select-rows #(= breakdown              (:breakdown              %)))
      (tc/select-columns [label-column :calendar-year value-key])
      (tc/pivot->wider [:calendar-year] [value-key])
      (tc/rename-columns column-names->labels)))


;;; # Example usage
(comment ; Example usage

  ;;; CPU pool to use
  (def cpu-pool (cp/threadpool (- (cp/ncpus) 2))) 

  ;;; Historical and simulated transitions
  (def in-dir "Relative path to directory containing census and projections" "../input")
  (def historical-transitions-file (str in-dir "transitions.csv"))
  (def simulated-transitions-files (summary-report/simulated-transitions-files "baseline-results" in-dir))

  ;;; Setup census-transform-f
  (def setting-category->dsg-placement-category
    "Map LA specific setting category code to corresponding Mastodon C DSG
     Management Plan 2022-23 (Version 5) placement category code"
    {"Mm"       "MmSoA"
     "MmI"      "MmSoA"
     "AC"       "RPSEN"
     "RBP"      "RPSEN"
     "MdSS"     "MdSSA"
     "EYS"      "NMISS"
     "InSS"     "NMISS"
     "Hosp"     "HspAP"
     "PRU"      "HspAP"
     "GFE"      "P16FE"
     "P16OTHER" "P16FE"
     "SP16"     "P16FE"
     "T"        "P16FE"
     "AWP"      "OTHER"
     "EHE"      "OTHER"
     "EOTAS"    "OTHER"
     "NEET"     "OTHER"
     "OTHER"    "OTHER"})

  (defn census-transform-f
    "Function to process the transitions for a single
     simulation (including history), after conversion to a census
     dataset, to have the required `:calendar-year`s,
     `:dsg-placement-category`s, `:age-group`s, and `:need`s."
    [ds]
    (-> ds
        (tc/map-columns :setting-category [:setting] #(re-find #"^[^_]+" %))
        (tc/map-columns :dsg-placement-category [:setting-category] #(setting-category->dsg-placement-category % nil))
        (tc/drop-rows (fn [row] (< 20 (:academic-year row))))))

  ;;; Calculate category stats, complete for dsg-categories and summarise to dsg-plan dataset
  (def category-stats
    "Stats ([`:sum` `:n-obs` `:n-sims` `:mean`] of `:transition-count`)
  for the [`:calendar-year` `:dsg-placement-category` `:age-group` `:need`]
  groups seen in the historical & simulated data."
    (transitions-files->category-stats historical-transitions-file
                                       simulated-transitions-files
                                       (partial simulation-transform-f census-transform-f)
                                       {:cpu-pool cpu-pool}))

  (def complete-dsg-category-stats
    "Stats ([`:sum` `:n-obs` `:n-sims` `:mean`] of `:transition-count`)
  for the complete set of `:calendar-year`s and DSG categories
  [`:dsg-placement-category` `:age-group` `:need`], required to
  complete the DSG Management Plan.
  Any non-DSG category combinations are dropped."
    (category-stats->complete-dsg-category-stats category-stats))

  (def dsg-plan
    (complete-dsg-category-stats->dsg-plan complete-dsg-category-stats))

  ;;; Extract individual breakdowns for display
  (extract-dsg-plan-breakdown dsg-plan "MmSoA" :age-group :rounded-mean :age-group-label)

  )
