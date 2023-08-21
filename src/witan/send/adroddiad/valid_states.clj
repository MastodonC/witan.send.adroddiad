(ns witan.send.adroddiad.valid-states
  (:require [tablecloth.api :as tc]
            [witan.send.domain.academic-years :as ay]))

(defn phase-min-ay [y]
  (cond
    (ay/nursery y) (apply min ay/nursery)
    (ay/primary-school y) (apply min ay/primary-school)
    (ay/secondary-school y) (apply min ay/secondary-school)
    (ay/post-16 y) (apply min ay/post-16)
    (ay/post-19 y) (apply min ay/post-19)))

(defn phase-max-ay [y]
  (cond
    (ay/nursery y) (apply max ay/nursery)
    (ay/primary-school y) (apply max ay/primary-school)
    (ay/secondary-school y) (apply max ay/secondary-school)
    (ay/post-16 y) (apply max ay/post-16)
    (ay/post-19 y) (apply max ay/post-19)))

(defn candidate-valid-states [census-data]
  (let [needs (->> (into (sorted-set) (:need census-data))
                   (interpose \,)
                   (apply str))
        settings (->> (into (sorted-set) (:setting census-data))
                      (interpose \,)
                      (apply str))
        max-ncy (->> census-data
                     :academic-year
                     (apply max))
        min-ncy (->> census-data
                     :academic-year
                     (apply min))]
    (-> census-data
        (tc/select-columns [:setting :academic-year])
        (tc/group-by (fn [row] (-> (:setting row)
                                   (clojure.string/split #"_")
                                   first)))
        (tc/aggregate {:min-academic-year #(max (phase-min-ay (reduce min (:academic-year %))) min-ncy)
                       :max-academic-year #(min (phase-max-ay (reduce max (:academic-year %))) max-ncy)})
        (tc/rename-columns {:$group-name :simple-setting})
        (tc/left-join (-> census-data
                          (tc/select-columns [:setting])
                          (tc/map-columns :simple-setting [:setting] #(-> %
                                                                          (clojure.string/split #"_")
                                                                          first))) :simple-setting)
        (tc/select-columns [:setting :min-academic-year :max-academic-year])
        (tc/unique-by)
        (tc/add-column :setting-group "All Settings")
        (tc/add-column :needs needs)
        (tc/add-column :setting->setting settings)
        (tc/select-columns [:setting :setting-group :min-academic-year :max-academic-year :needs :setting->setting])
        (tc/order-by [:setting]))))

(defn create-candidate-valid-states! [census-data out-file-name]
  (-> census-data
      candidate-valid-states
      (tc/write! out-file-name)))
