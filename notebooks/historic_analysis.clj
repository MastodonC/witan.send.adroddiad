^{:nextjournal.clerk/toc true}
(ns historic-analysis
  {
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/open-graph
   {:image "https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/favicon.png"}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.clerk.html :as chtml]
            [witan.send.adroddiad.dataset :as ds]))

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
(def census (tc/dataset (str in-dir nil) (:key-fn keyword))) ;; consider defaulting to filename
(def transitions (tc/dataset (str in-dir nil) {:key-fn keyword})) ;; consider defaulting to filename

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

{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# " client-name " SEND " sen2-calendar-year " Validation " date-string))
