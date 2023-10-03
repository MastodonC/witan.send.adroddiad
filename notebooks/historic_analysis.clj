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

(def field-descriptions
  {:setting :setting-label
   :setting-1 :setting-1-label
   :setting-2 :setting-2-label
   :need :need-label
   :need-1 :need-1-label
   :need-2 :need-2-label
   :academic-year :academic-year-label
   :academic-year-1 :academic-year-1-label
   :academic-year-2 :academic-year-2-label})

(def sweet-column-names
  {:calendar-year         "Calendar Year"
   :diff                  "Count"
   :row-count             "Row Count"
   :pct-change            "% Change"
   :setting-label         "Setting"
   :need-label            "Need"
   :academic-year         "NCY"
   :academic-year-label   "NCY"
   :setting-label-1       "Setting 1"
   :need-label-1          "Need 1"
   :academic-year-1       "NCY 1"
   :academic-year-1-label "NCY 1"
   :setting-label-2       "Setting 2"
   :need-label-2          "Need 2"
   :academic-year-2-label "NCY 2"
   :setting-1             "Setting 1"
   :setting-2             "Setting 2"})

(def axis-labels
  {:setting "Setting"
   :need "Need"
   :calendar-year "Calendar Year"
   :setting-1 "Setting 1"
   :setting-2 "Setting 2"
   :academic-year-1 "NCY 1"
   :academic-year-2 "NCY 2"})

(def sort-field
  {:setting :setting-order
   :setting-1 :setting-1-order
   :setting-2 :setting-2-order
   :need :need-order
   :need-1 :need-1-order
   :need-2 :need-2-order
   :academic-year :academic-year-order
   :academic-year-1 :academic-year-1-order
   :academic-year-2 :academic-year-2-order})

{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# " client-name " SEND " sen2-calendar-year " Validation " date-string))
