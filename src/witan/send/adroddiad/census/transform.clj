(ns witan.send.adroddiad.census.transform
  (:require [tablecloth.api :as tc]))

(defn census->transitions [census-data]
  (let [[first-year last-year] ((juxt first last) (into (sorted-set) (-> census-data :calendar-year)))
        s1 (-> census-data
               (tc/set-dataset-name "s1")
               (tc/drop-rows #(= last-year (:calendar-year %)))
               (tc/rename-columns {:need :need-1 :setting :setting-1 :academic-year :academic-year-1}))
        s2 (-> census-data
               (tc/set-dataset-name "s2")
               (tc/drop-rows #(= first-year (:calendar-year %)))
               (tc/rename-columns {:need :need-2 :setting :setting-2 :academic-year :academic-year-2})
               (tc/map-columns :calendar-year [:calendar-year] (fn [cy] (dec cy))))]
    (-> (tc/full-join s1 s2 [:id :calendar-year])
        (tc/map-columns :id [:id :s2.id] (fn [id s2-id] (if id id s2-id)))
        (tc/map-columns :calendar-year [:calendar-year :s2.calendar-year] (fn [calendar-year s2-calendar-year] (if calendar-year calendar-year s2-calendar-year)))
        (tc/map-columns :academic-year-1 [:academic-year-1 :academic-year-2] (fn [ay1 ay2] (if ay1 ay1 (dec ay2))))
        (tc/map-columns :academic-year-2 [:academic-year-1 :academic-year-2] (fn [ay1 ay2] (if ay2 ay2 (inc ay1))))
        (tc/replace-missing [:setting-1 :need-1 :setting-2 :need-2] :value "NONSEND")
        (tc/select-columns [:id :calendar-year
                            :setting-1 :need-1 :academic-year-1
                            :setting-2 :need-2 :academic-year-2])
        (tc/order-by [:id :calendar-year])
        tc/clone)))
