(ns witan.send.adroddiad.clerk.viewer
  "Custom clerk viewers and viewer utility functions."
  (:require
   [nextjournal.clerk.viewer :as clerk-viewer]))

(defn transform-child-viewers [viewer & update-args]
  (update viewer :transform-fn
          (partial comp #(apply update % :nextjournal/viewers
                                clerk-viewer/update-viewers update-args))))

(def table-viewer-no-elision
  "clerk table viewer without elision at any level"
  ;; See: https://clojurians.slack.com/archives/C035GRLJEP8/p1678874466399499?thread_ts=1675945066.743059&cid=C035GRLJEP8
  (transform-child-viewers clerk-viewer/table-viewer
                           {:page-size #(dissoc % :page-size)}))
