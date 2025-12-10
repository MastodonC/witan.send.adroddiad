(ns witan.send.adroddiad.domain.need
  "Definitions and functions for handling EHCP needs."
  (:require [tablecloth.api :as tc]))



;;; # Utility functions
(defn key-comparator-fn-by
  [key->order]
  (fn [k1 k2]
    (compare [(key->order k1) k1]
             [(key->order k2) k2])))



;;; # Needs
(def dictionary
  "EHCP need dictionary"
  (as-> {"ASD"  {:order      1
                 :name       "Autistic Spectrum Disorder"
                 :label      "ASD"
                 :definition "Autistic spectrum disorder"
                 :sen2-order 11
                 :send-area  "CnI"}
         "SLCN" {:order      2
                 :name       "Speech, Language and Communication Needs"
                 :label      "SLCN"
                 :definition "Speech, language and communication needs"
                 :sen2-order 6
                 :send-area  "CnI"}
         "SEMH" {:order      3
                 :name       "Social, Emotional and Mental Health"
                 :label      "SEMH"
                 :definition "Social, emotional and mental health"
                 :sen2-order 5
                 :send-area  "SEM"}
         "SPLD" {:order      4
                 :name       "Specific Learning Difficulty"
                 :label      "SpLD"     ; Note lower case "p"
                 :definition "Specific learning difficulty"
                 :sen2-order 1
                 :send-area  "CnL"}
         "MLD"  {:order      5
                 :name       "Moderate Learning Difficulty"
                 :label      "MLD"
                 :definition "Moderate learning difficulty"
                 :sen2-order 2
                 :send-area  "CnL"}
         "SLD"  {:order      6
                 :name       "Severe Learning Difficulty"
                 :label      "SLD"
                 :definition "Severe learning difficulty"
                 :sen2-order 3
                 :send-area  "CnL"}
         "PMLD" {:order      7
                 :name       "Profound and Multiple Learning Difficulty"
                 :label      "PMLD"
                 :definition "Profound and multiple learning difficulty"
                 :sen2-order 4
                 :send-area  "CnL"}
         "DS"   {:order      8
                 :name       "Down Syndrome"
                 :label      "DS"
                 :definition "Down Syndrome"
                 :sen2-order 12
                 :send-area  "CnL"}
         "HI"   {:order      9
                 :name       "Hearing Impairment"
                 :label      "HI"
                 :definition "Hearing impairment"
                 :sen2-order 7
                 :send-area  "SPN"}
         "VI"   {:order      10
                 :name       "Vision Impairment"
                 :label      "VI"
                 :definition "Vision impairment"
                 :sen2-order 8
                 :send-area  "SPN"}
         "MSI"  {:order      11
                 :name       "Multi-Sensory Impairment"
                 :label      "MSI"
                 :definition "Multi-sensory impairment"
                 :sen2-order 9
                 :send-area  "SPN"}
         "PD"   {:order      12
                 :name       "Physical Disability"
                 :label      "PD"
                 :definition "Physical disability"
                 :sen2-order 10
                 :send-area  "SPN"}
         "OTH"  {:order      13
                 :name       "Other Difficulty"
                 :label      "OTH"
                 :definition "Other difficulty"
                 :sen2-order 13
                 :send-area  "OTH"}} $
    (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $)))

(def ^:deprecated needs dictionary)

(def abbreviations
  "EHCP need abbreviations as a sorted set"
  (into (sorted-set-by (key-comparator-fn-by (update-vals dictionary :order)))
        (keys dictionary)))

(def dataset
  "EHCP need dictionary as a dataset."
  (as-> dictionary $
    (map (fn [[k v]] (assoc v :abbreviation k)) $)
    (tc/dataset $)
    (tc/reorder-columns $ [:abbreviation])
    (tc/set-dataset-name $ "needs")))

(def ^:deprecated needs-ds dataset)