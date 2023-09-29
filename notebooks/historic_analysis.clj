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
(def la nil)
(def year nil)
(def date nil)

{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# " la " SEND " year " Validation " date))
