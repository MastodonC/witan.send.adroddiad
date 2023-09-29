^{:nextjournal.clerk/toc true}
(ns historic-analysis
  {
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/open-graph
   {:title "Hello World!" #_(format "MC:%s %s %s %s" LA date work-package title)
    :image "https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/favicon.png"}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]))

^{:nextjournal.clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# Hounslow SEND Setting Processing README 2023-10-05"))
