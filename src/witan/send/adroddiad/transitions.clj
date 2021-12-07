(ns witan.send.adroddiad.transitions
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]))

(defn leaver? [transition]
  (= "NONSEND" (get transition :setting-2)))

(defn joiner? [transition]
  (= "NONSEND" (get transition :setting-1)))

(defn mover? [transition]
  (and (not (joiner? transition))
       (not (leaver? transition))
       (not= (:setting-1 transition) (:setting-2 transition))))

(defn leavers-from
  ([simulation-results setting]
   (-> simulation-results
       (tc/select-rows #(= setting (:setting-1 %)))
       (tc/select-rows leaver?)))
  ([simulation-results]
   (-> simulation-results
       (tc/select-rows #(= (:setting-1 %) (:setting-1 %)))
       (tc/select-rows leaver?))))

(defn movers-to
  ([simulation-results setting]
   (-> simulation-results
       (tc/select-rows mover?)
       (tc/select-rows #(= setting (:setting-2 %)))))
  ([simulation-results]
   (-> simulation-results
       (tc/select-rows mover?)
       (tc/select-rows #(= (:setting-2 %) (:setting-2 %))))))

(defn movers-from [simulation-results setting]
  (-> simulation-results
      (tc/select-rows mover?)
      (tc/select-rows #(= setting (:setting-1 %)))))

(defn joiners-to
  ([simulation-results setting]
   (-> simulation-results
       (tc/select-rows #(= setting (:setting-2 %)))
       (tc/select-rows joiner?)
       (tc/map-columns :calendar-year [:calendar-year] #(dec %))))
  ([simulation-results]
   (-> simulation-results
       (tc/select-rows #(= (:setting-2 %) (:setting-2 %)))
       (tc/select-rows joiner?)
       (tc/map-columns :calendar-year [:calendar-year] #(dec %)))))

(defn min-calendar-year [transitions]
  (dfn/reduce-min (:calendar-year transitions)))

(defn transitions->census
  ([transitions start-year]
   (let [year-1-census (-> transitions
                           (tc/select-rows #(= (:calendar-year %) start-year))
                           (tc/drop-columns [:setting-2 :need-2 :academic-year-2])
                           (tc/rename-columns {:setting-1 :setting
                                               :need-1 :need
                                               :academic-year-1 :academic-year}))]
     (-> transitions
         (tc/map-columns :calendar-year-2 [:calendar-year] #(inc %))
         (tc/drop-columns [:calendar-year :setting-1 :need-1 :academic-year-1])
         (tc/rename-columns {:calendar-year-2 :calendar-year
                             :setting-2 :setting
                             :need-2 :need
                             :academic-year-2 :academic-year})
         (tc/concat year-1-census)
         (tc/drop-rows #(= "NONSEND" (:setting %))))))
  ([transitions]
   (transitions->census transitions (min-calendar-year transitions))))
