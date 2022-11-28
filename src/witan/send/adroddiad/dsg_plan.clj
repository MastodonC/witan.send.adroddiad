(ns witan.send.adroddiad.dsg-plan
  (:require [witan.send.adroddiad.ncy :as ncy]
            [com.climate.claypoole.lazy :as lazy]
            [tablecloth.api :as tc]
            [tech.v3.dataset.reductions :as ds-reduce]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary.report :as summary-report]))


(defn age-group
  "Age groups for DSG management plan

  From the \"Introduction\" tab of the DSG Management Plan template v5
  and per section 2, part 1, item 1.1 of the [2022 SEN2
  guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf).

  Age is age in whole number of years on 31st August prior to starting the school/academic year."
  [age]
  (cond
    (< age 5) "Under 5"
    (<= 5 age 10) "Age 5 to 10"
    (<= 11 age 15) "Age 11 to 15"
    (<= 16 age 19) "Age 16 to 19"
    (<= 20 age 25) "Age 20 to 25"
    (< 25 age) "Over 25"))

(defn summarise-setting-by-age-group [census]
  (-> census
      (tc/map-columns :age-group [:academic-year]
                      (fn [ay]
                        (-> ay
                            ncy/ncy->age-at-start-of-school-year
                            age-group)))
      (tc/group-by [:calendar-year :setting :age-group])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (tc/complete :calendar-year :setting :age-group)
      (tc/replace-missing :transition-count :value 0)))

(defn summarise-setting-by-need [census]
  (-> census
      (tc/group-by [:calendar-year :setting :need])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (tc/complete :calendar-year :setting :need)
      (tc/replace-missing :transition-count :value 0)))

(defn transform-simulations
  [{:keys [simulations
           simulations-transform-f
           cpu-pool]
    :or {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (lazy/upmap cpu-pool simulations-transform-f simulations))

(defn summarise-simulations [{:keys [simulations
                                     domain-keys
                                     value-key]}]
  (ds-reduce/group-by-column-agg
   domain-keys
   {:sum       (ds-reduce/sum value-key)
    :row-count (ds-reduce/row-count)
    :mean      (ds-reduce/mean value-key)
    :median    (ds-reduce/prob-median value-key)}
   simulations))

(defn historical-transitions->simulated-counts [transition-file]
  (-> transition-file
      (tc/dataset {:key-fn keyword})
      (tc/group-by [:calendar-year
                    :academic-year-1 :need-1 :setting-1
                    :academic-year-2 :need-2 :setting-2])
      (tc/aggregate {:transition-count tc/row-count})
      (tc/map-columns :calendar-year-2 [:calendar-year] inc)
      (tc/add-column :simulation -1)
      (tc/convert-types {:academic-year-1 :int8
                         :academic-year-2 :int8
                         :calendar-year :int16
                         :calendar-year-2 :int16
                         :simulation :int8
                         :transition-count :int64})))

(defn simulations-seq
  "Returns a sequence of datasets where each dataset represents the
  transition-counts for a single simulation. It is a lazy sequence and
  can be streamed to ds-reduce."
  [{:keys [historical-transitions-file
           simulated-transitions-files
           cpu-pool]
    :or {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (let [history (historical-transitions->simulated-counts historical-transitions-file)]
    (sequence
     (comp
      cat
      (map (fn [ds] (tc/concat history ds))))
     (lazy/upmap cpu-pool
                 (partial summary-report/read-and-split :simulation)
                 simulated-transitions-files))))

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
