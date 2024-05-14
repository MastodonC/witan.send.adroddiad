(ns witan.send.adroddiad.analysis.setting-by-phase
  (:require
   [witan.send.adroddiad.vega-specs :as vs]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.analysis.total-domain :as td]
   [witan.send.domain.academic-years :as day]))

(def setting-phase-summary-defaults
  {:domain-key [:phase :setting]
   :extra-transformation-f
   (fn [ds]
     (tc/map-columns ds :phase [:academic-year] day/ncy->school-phase-name))})

(defn summarise-from-config
  "This needs the path to a config.edn and a parquet prefix for the results."
  [config]
  (td/summarise-from-config
   (merge config setting-phase-summary-defaults)))

(def color-shapes-order
  (-> ["Nursery" "Primary" "Secondary" "Post 16" "Post 19"]
      (vs/color-and-shape-lookup)
      (tc/add-column :order (range))))

(defn total-summary-plot [summary setting]
  (td/total-summary-plot
   {:data (-> summary 
              (tc/select-rows #(= setting (:setting %)))
              (tc/inner-join color-shapes-order [:phase :domain-value]))
    :order-field :order
    :label-field :phase
    :colors-and-shapes color-shapes-order}))
