(ns witan.send.adroddiad.analysis.ehcp-by-phase
  (:require
   [tablecloth.api :as tc]
   [witan.send.adroddiad.analysis.total-domain :as td]
   [witan.send.domain.academic-years :as day]))

(def phase-summary-defaults
  {:domain-key [:phase]
   :extra-transformation-f
   (fn [ds]
     (tc/map-columns ds :phase [:academic-year] day/ncy->school-phase-name))})

;;; FIXME: This really should be against background population in
;;; addition to just the percentage of EHCPs
(defn summarise-from-config
  "This needs the path to a config.edn and a parquet prefix for the results."
  [config]
  (td/summarise-from-config
   (merge config phase-summary-defaults)))
