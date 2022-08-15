(ns witan.send.adroddiad.dsg-plan
  (:require [com.climate.claypoole.lazy :as lazy]
            [witan.send.adroddiad.summary :as summary]
            [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.summary.report :as summary-report]
            [witan.send.adroddiad.transitions :as at]))

;; Dataset needs to be in a canonical census with transition-count shape for this to work 
(defn ncy->age
  "Taken from witan.send.domain.academic-years to avoid a dependency"
  [ncy]
  (+ ncy 5))

(defn age->ncy
  "Taken from witan.send.domain.academic-years to avoid a dependency"
  [age]
  (- age 5))

(defn age-group
  "Taken from witan.send.domain.academic-years to avoid a dependency"
  [age]
  (cond
    (< age 5) "Age 0 to 5"
    (<= 5 age 10) "Age 05 to 10"
    (<= 11 age 15) "Age 11 to 15"
    (<= 16 age 19) "Age 16 to 19"
    (<= 20 age 25) "Age 20 to 25"
    (< 25 age) "Over 25"))

(defn summarise-setting-by-age [census]
  (-> census
      (tc/map-columns :age-group [:academic-year]
                      (fn [ay]
                        (-> ay
                            ncy->age
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


(defn summarise-simulations
  [{:keys [cpu-pool ;; Allows us to share the resources between all the jobs
           simulations ;; history + all simulations from parquet
           domain-keys                  ;; [:calendar-year :age-group :setting-category] or [:calendar-year :age-group :need]
           order-keys ;; :calendar-year ;; this is just the x-key
           value-key ;; :transition-count
           simulation-transform]
    :or {cpu-pool (java.util.concurrent.ForkJoinPool/commonPool)}}]
  (as-> simulations $
    (lazy/upmap cpu-pool simulation-transform $)
    (summary/seven-number-summary $ domain-keys value-key)
    (tc/order-by $ order-keys)))

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
  (conj
   (sequence
    cat
    (lazy/upmap cpu-pool
                (partial summary-report/read-and-split :simulation)
                simulated-transitions-files))
   (historical-transitions->simulated-counts historical-transitions-file)))
