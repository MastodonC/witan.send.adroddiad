(ns witan.send.adroddiad
  (:require [witan.send.adroddiad.census :as census]
            [witan.send.adroddiad.single-population :as single-population]))

;; This looks like a job for Potemkin
(def single-population-report single-population/single-population-report)
(def census-report census/census-report)
