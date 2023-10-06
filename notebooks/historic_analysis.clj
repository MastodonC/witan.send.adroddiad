^{:nextjournal.clerk/toc true}
(ns historic-analysis
  {:nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.clerk.html :as chtml]
            [witan.send.adroddiad.dataset :as ds]
            [witan.send.adroddiad.clerk.charting-v2 :as chart]))

{:nextjournal.clerk/visibility {:result :hide}}
(
 ;; Template input section
 )

(def client-name nil)
(def sen2-calendar-year nil)
(def date-string nil)
(def out-dir nil)
(def workpackage-name nil)
(def in-dir nil)
(def census (tc/dataset (str in-dir nil) {:key-fn keyword}))
(def transitions (tc/dataset (str in-dir nil) {:key-fn keyword}))

(
 ;; Supporting functions and defs
 )

(clerk/add-viewers! [slideshow/viewer])

(comment
  ;; Writes out to to a standalone html file
  (chtml/ns->html out-dir *ns*)

  )

(
;;; Charting helpers
 )

(def full-height 600)
(def two-rows 200)
(def half-width 600)
(def full-width 1420)

(def chart-base
  {:data              nil
   :chart-height      full-height    :chart-width full-width
   :clerk-width       :full
   :chart-title       nil
   :range-format-f    (fn [lower upper]
                        (format "%,.2f - %,.2f" lower upper))
   :x                 :calendar-year :x-title     "Year"    :x-format "%Y"
   :y                 :median        :y-title     "# EHCPs" :y-format "%,.2f" :y-zero true
   :irl               :q1            :iru         :q3       :ir-title "50% range"
   :orl               :p05           :oru         :p95      :or-title "90% range"
   :group             :baseline      :group-title nil
   :colors-and-shapes nil})

(
 ;; Notebook
 )

{:nextjournal.clerk/visibility {:result :show}}
;; ---
(clerk/html
 {::clerk/width :full}
 [:div.max-w-screen-2xl.font-sans
  [:img {:src "https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png"}]
  [:p.text-7xl.font-extrabold.-mb-8.-mt-8 (format "%1s Historical Analysis for %2s" workpackage-name sen2-calendar-year)]
  [:p.text-3xl.italic date-string]
  [:p.text-5xl.font-bold.-mb-8 (format "For %s" client-name)]
  [:p.text-4xl.font-bold.italic "Presented by Mastodon C"]
  [:p.text-3xl "Use ⬅️➡️ keys to navigate and ESC to see an overview."]])

;; ---
;; ## TBD Conclusion

(clerk/row
 {::clerk/width :full}
 (chart/ehcps-total-by-year census)
 (chart/echps-total-yoy-change census)
 (chart/echps-total-yoy-pct-change census))

;; ---
;; ## TBD Conclusion

(chart/ehcps-by-setting-per-year census)

;; ---
;; ## TBD Conclusion

(chart/ehcp-by-need-by-year census)

;; ---
;; ## TBD Conclusion

(chart/ehcps-by-ncy-per-year census)

;; ---
;; ## TBD Conclusion

(clerk/row
 {::clerk/width :full}
 (chart/ehcps-by-setting-yoy-change census)
 (chart/ehcps-by-setting-yoy-pct-change census))

;; ---
;; ## TBD Conclusion

(clerk/row
 {::clerk/width :full}
 (chart/ehcps-by-need-yoy-change census)
 (chart/ehcps-by-need-yoy-pct-change census))

;; ---
;; ## TBD Conclusion

(clerk/row
 {::clerk/width :full}
 (chart/ehcps-by-ncy-yoy-change census)
 (chart/ehcps-by-ncy-yoy-pct-change census))

;; ---
;; ## TBD Conclusion

(chart/joiners-by-ehcp-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/joiners-by-ncy-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/joiners-by-need-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/joiners-by-setting-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/leavers-by-ehcp-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/leavers-by-ncy-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/leavers-by-setting-per-year transitions)

;; ---
;; ## TBD Conclusion

(chart/setting-to-setting-heatmap transitions)

;; ---
;; ## TBD Conclusion

(chart/setting-mover-heatmap transitions)

;; ---
;; ## TBD Conclusion

(chart/joiners-by-setting-and-ncy transitions)

;; ---
;; ## TBD Conclusion

(chart/needs-by-designation census)
