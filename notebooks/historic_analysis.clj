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
            [witan.send.adroddiad.clerk.html :as chtml]))

(
 ;; Template input section
 )

{:nextjournal.clerk/visibility {:result :hide}}
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


{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# " client-name " SEND " sen2-calendar-year " Validation " date-string))
