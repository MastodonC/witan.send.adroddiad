(ns witan.send.adroddiad.clerk
  "Custom `clerk` functions."
  (:require
   [nextjournal.clerk :as clerk]
   [witan.send.adroddiad.clerk.viewer :as adroddiad.clerk.viewer]))

(defn table-no-elision
  "table with no elision at any level"
  ([xs] (table-no-elision {} xs))
  ([viewer-opts xs]
   (clerk/with-viewer
     adroddiad.clerk.viewer/table-viewer-no-elision
     viewer-opts
     xs)))