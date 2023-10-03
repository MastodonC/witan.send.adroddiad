^{:nextjournal.clerk/toc true}
(ns historic-analysis
  {
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/open-graph
   {:image "https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/favicon.png"}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]))

{:nextjournal.clerk/visibility {:result :hide}}
(def client-name nil)
(def sen2-calendar-year nil)
(def date-string nil)
(def out-dir nil)
(def workpackage-name nil)

{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# " client-name " SEND " sen2-calendar-year " Validation " date-string))
