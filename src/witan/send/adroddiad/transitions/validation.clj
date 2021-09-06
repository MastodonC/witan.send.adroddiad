(ns witan.send.adroddiad.transitions.validation
  (:require [kixi.large :as large]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.transitions :as t]))

(defn setting-count [census]
  (-> census
      (tc/group-by [:calendar-year :setting])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:setting :calendar-year :population])
      (tc/order-by [:setting :calendar-year])))

(defn need-counts [census]
  (-> census
      (tc/group-by [:calendar-year :need])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:need :calendar-year :population])
      (tc/order-by [:need :calendar-year])))

(defn ncy-count [census]
  (-> census
      (tc/group-by [:calendar-year :academic-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:academic-year :calendar-year :population])
      (tc/order-by [:academic-year :calendar-year])))

(defn joiners-by-calendar-year [transitions]
  (-> transitions
      (tc/select-rows t/joiner?)
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:calendar-year :population])
      (tc/order-by [:calendar-year])))

(defn movers-by-calendar-year [transitions]
  (-> transitions
      (tc/select-rows t/mover?)
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:calendar-year :population])
      (tc/order-by [:calendar-year])))

(defn leavers-by-calendar-year [transitions]
  (-> transitions
      (tc/select-rows t/leaver?)
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:calendar-year :population])
      (tc/order-by [:calendar-year])))

(defn transitions-by-calendar-year [transitions]
  (-> transitions
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:calendar-year :population])
      (tc/order-by [:calendar-year])))

(defn joiners-by-ncy [transitions]
  (-> transitions
      (tc/select-rows t/joiner?)
      (tc/group-by [:calendar-year :academic-year-2])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:academic-year-2 :calendar-year :population])
      (tc/order-by [:calendar-year :academic-year-2])))

(defn movers-by-ncy [transitions]
  (-> transitions
      (tc/select-rows t/mover?)
      (tc/group-by [:calendar-year :academic-year-2])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:academic-year-2 :calendar-year :population])
      (tc/order-by [:calendar-year :academic-year-2])))

(defn leavers-by-ncy [transitions]
  (-> transitions
      (tc/select-rows t/leaver?)
      (tc/group-by [:calendar-year :academic-year-1])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:academic-year-1 :calendar-year :population])
      (tc/order-by [:calendar-year :academic-year-1])))

(defn population-by-year [census]
  (-> census
      (tc/group-by [:calendar-year])
      (tc/aggregate {:population tc/row-count})
      (tc/select-columns [:calendar-year :population])
      (tc/order-by [:calendar-year])))

(defn joiner-leaver-population [transitions]
  (-> (-> transitions
          population-by-year)
      (tc/left-join (-> transitions
                        joiners-by-calendar-year
                        (tc/rename-columns {:population :joiners}))
                    [:calendar-year])
      (tc/left-join (-> transitions
                        leavers-by-calendar-year
                        (tc/rename-columns {:population :leavers}))
                    [:calendar-year])
      (tc/map-columns :joiner-leaver-delta [:joiners :leavers] #(- %1 %2))
      (tc/map-columns :upcoming-population [:population :joiner-leaver-delta] +)
      (tc/select-columns [:calendar-year :population :upcoming-population :joiner-leaver-delta :joiners :leavers])
      (tc/order-by [:calendar-year])))

(defn report [{:keys [census transitions]}]
  [{::large/sheet-name "Needs"
    ::large/data (need-counts census)}
   {::large/sheet-name "Settings"
    ::large/data (setting-count census)}
   {::large/sheet-name "NCYs"
    ::large/data (ncy-count census)}
   {::large/sheet-name "Joiners by NCY"
    ::large/data (joiners-by-ncy transitions)}
   {::large/sheet-name "Movers by NCY"
    ::large/data (movers-by-ncy transitions)}
   {::large/sheet-name "Leavers by NCY"
    ::large/data (leavers-by-ncy transitions)}
   {::large/sheet-name "Joiners and Leavers"
    ::large/data (joiner-leaver-population transitions)}
   {::large/sheet-name "Total pop by year"
    ::large/data (population-by-year census)}
   {::large/sheet-name "Census"
    ::large/data census}
   {::large/sheet-name "Transitions"
    ::large/data transitions}])
