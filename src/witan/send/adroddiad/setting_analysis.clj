(ns witan.send.adroddiad.setting-analysis
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [witan.send.adroddiad.simulated-transition-counts :as stc]
            [witan.send.adroddiad.summary :as summary]
            [witan.send.adroddiad.transitions :as tr]))

(defn count-census-by-year [census-simuation-data]
  (-> census-simuation-data
      (tc/group-by [:simulation :calendar-year])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (summary/seven-number-summary [:calendar-year] :transition-count)
      (tc/order-by [:calendar-year])))

(defn setting-analysis [simulations setting]
  (let [min-year (reduce min (-> simulations
                                 (tc/select-rows #(= -1 (:simulation %)))
                                 :calendar-year))]
    (-> simulations
        (stc/transition-counts->census-counts min-year)
        (tc/select-rows #(= setting (:setting %)))
        count-census-by-year
        (tc/select-columns [:calendar-year :median])
        (tc/rename-columns {:median :median-total})
        (tc/left-join (-> simulations
                          (tr/joiners-to setting)
                          count-census-by-year
                          (tc/select-columns [:calendar-year :median])
                          (tc/rename-columns {:median :median-joiners-to}))
                      [:calendar-year])
        (tc/left-join (-> simulations
                          (tr/movers-to setting)
                          count-census-by-year
                          (tc/select-columns [:calendar-year :median])
                          (tc/rename-columns {:median :median-movers-to}))
                      [:calendar-year])
        (tc/left-join (-> simulations
                          (tr/movers-from setting)
                          count-census-by-year
                          (tc/select-columns [:calendar-year :median])
                          (tc/rename-columns {:median :median-movers-from}))
                      [:calendar-year])
        (tc/left-join (-> simulations
                          (tr/leavers-from setting)
                          count-census-by-year
                          (tc/select-columns [:calendar-year :median])
                          (tc/rename-columns {:median :median-leavers-from}))
                      [:calendar-year])
        (tc/map-columns :inflow [:median-joiners-to :median-movers-to] +)
        (tc/map-columns :outflow [:median-leavers-from :median-movers-from] +)
        (tc/map-columns :net-increase [:inflow :outflow] #(- %1 %2))
        (tc/map-columns :next-year-total [:median-total :net-increase] +)
        (tc/select-columns [:calendar-year :median-total :next-year-total :net-increase :inflow :outflow :median-joiners-to :median-movers-to :median-leavers-from :median-movers-from])
        (tc/rename-columns {:calendar-year "Calendar Year at January"
                            :median-total "Median Total Population"
                            :net-increase "Net Increase"
                            :inflow "Inflow"
                            :outflow "Outflow"
                            :median-joiners-to "Median Joiners To"
                            :median-movers-to "Median Movers In"
                            :median-leavers-from "Median Leavers From"
                            :median-movers-from "Median Movers From"})
        (tc/order-by ["Calendar Year at January"]))))
