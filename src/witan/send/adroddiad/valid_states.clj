(ns witan.send.adroddiad.valid-states
  (:require [tablecloth.api :as tc]))

(defn candidate-valid-states [census-data]
  (let [needs (->> (into (sorted-set) (:need census-data))
                   (interpose \,)
                   (apply str))
        settings (->> (into (sorted-set) (:setting census-data))
                      (interpose \,)
                      (apply str))]
    (-> census-data
        (tc/select-columns [:setting :academic-year])
        (tc/group-by (fn [row] (-> (:setting row)
                                   (clojure.string/split #"_")
                                   first)))
        (tc/aggregate {:min-academic-year #(reduce min (:academic-year %))
                       :max-academic-year #(reduce max (:academic-year %))})
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
