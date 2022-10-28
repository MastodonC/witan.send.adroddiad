(ns witan.send.adroddiad.transitions
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]))

(defn leaver?
  ([setting-1 setting-2]
   (and (not= "NONSEND" setting-1)
        (= "NONSEND" setting-2)))
  ([{:keys [setting-1 setting-2]}]
   (leaver? setting-1 setting-2)))

(defn joiner?
  ([setting-1 setting-2]
   (and (= "NONSEND" setting-1)
        (not= "NONSEND" setting-2)))
  ([{:keys [setting-1 setting-2]}]
   (joiner? setting-1 setting-2)))

(defn mover?
  ([setting-1 setting-2]
   (and (not (joiner? setting-1 setting-2))
        (not (leaver? setting-1 setting-2))
        (not= setting-1 setting-2)))
  ([{:keys [setting-1 setting-2]}]
   (mover? setting-1 setting-2)))

(defn stayer?
  ([setting-1 setting-2]
   (and (not= "NONSEND" setting-1 setting-2)
        (= setting-1 setting-2)))
  ([{:keys [setting-1 setting-2]}]
   (stayer? setting-1 setting-2)))

(defn transition-type
  ([setting-1 setting-2]
   (cond
     (leaver? setting-1 setting-2) "leaver"
     (joiner? setting-1 setting-2) "joiner"
     (mover? setting-1 setting-2) "mover"
     (stayer? setting-1 setting-2) "stayer"
     :else "NONSEND"))
  ([{:keys [setting-1 setting-2]}]
   (transition-type setting-1 setting-2)))

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

(defn movers-from
  ([simulation-results setting]
   (-> simulation-results
       (tc/select-rows mover?)
       (tc/select-rows #(= setting (:setting-1 %)))))
  ([simulation-results]
   (-> simulation-results
       (tc/select-rows mover?)
       (tc/select-rows #(= (:setting-1 %) (:setting-1 %))))))

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
         (tc/drop-columns [:calendar-year-2])
         (tc/drop-rows #(= "NONSEND" (:setting %))))))
  ([transitions]
   (transitions->census transitions (min-calendar-year transitions))))

(defn census-counts->census-counts-complete
  "Given `census` dataset, returns aggregated dataset with
  transition-counts summed for each complete combination of
  [:calendar-year :setting :academic-year :need] including count of 0
  for combinations not in the `census`.

  Input dataset `census` must contain columns [:calendar-year :setting
  :academic-year :need :transition-counts]

  Note that the completion of [:calendar-year :setting :academic-year
  :need] is not contrained to valid-states."
  [census]
  (-> census
      (tc/group-by [:calendar-year :setting :academic-year :need])
      (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
      (tc/complete :setting :academic-year :need :calendar-year)
      (tc/replace-missing [:transition-count] :value 0)))

(defn transition-counts->census-counts-complete
  "Given `transitions` dataset, returns corresponding census dataset with
   :transition-counts summed for each complete combination of
   [:calendar-year :setting :academic-year :need] including count of 0
   for combinations not in the `transitions`.

   Note that the completion of [:calendar-year :setting :academic-year
   :need] is not contrained to valid-states."
  [transitions]
  (-> transitions
      transitions->census
      census-counts->census-counts-complete))
