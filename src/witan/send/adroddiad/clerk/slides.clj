(ns witan.send.adroddiad.clerk.slides
  (:require [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]))

(def mc-logo-url "https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png")

(defn mc-logo []
  (clerk/html
   {::clerk/width :full}
   [:div.max-w-screen-2xl.font-sans
    [:div.h-full.max-h-full.bottom-0.-right-12.absolute [:img {:src mc-logo-url}]]]))

(defn bulleted-list [seq-of-text]
  (reduce #(into %1 [[:li.text-3xl.mb-4.mt-4 %2]]) [:ul.list-disc] seq-of-text))

;; TODO
;; single slide fn that does different layouts based on a :slide (?) key

(defn title-slide [{:keys [presentation-title
                           work-package
                           presentation-date
                           client-name]
                    :or   {presentation-title ""
                           work-package ""
                           presentation-date ""
                           client-name ""}}]
  (clerk/fragment
   (clerk/html {::clerk/width :full}
               [:div.max-w-screen-2xl.font-sans
                [:p.text-6xl.font-extrabold presentation-title]
                [:p.text-3xl.font-bold work-package]
                [:p.text-3xl.italic presentation-date]
                [:p.text-5xl.font-bold.-mb-8.-mt-2 (format "For %s" client-name)]
                [:p.text-4xl.font-bold.italic "Presented by Mastodon C"]
                [:p.text-1xl "Use arrow keys to navigate and ESC to see an overview."]])
   (mc-logo)))

(defn list-slide [{:keys [title
                          text]
                   :or   {title "Title"
                          text ["Point 1"
                                "Point 2"
                                "Point 3"]}}]
  (clerk/fragment
   (clerk/html
    [:p.text-6xl.font-bold.font-sans title])
   (clerk/html
    {::clerk/width :full}
    [:div.text-1xl.max-w-screen-2xl.font-sans
     (bulleted-list text)])
   (mc-logo)))

(defn section-header-slide [{:keys [section-title]
                             :or   {section-title "Section Title"}}]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans section-title])
   (mc-logo)))

(defn title-chart-slide [{:keys [title
                                 chart
                                 text]
                          :or   {title "Title"
                                 chart {}
                                 text ["Point 1"
                                       "Point 2"
                                       "Point 3"]}}]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans title]) ;;WORKING HERE
   (clerk/vl {::clerk/width :full}
             chart)
   (mc-logo)))

(defn title-chart-text-slide [{:keys [title
                                      chart
                                      text]
                               :or   {title "Title"
                                      chart {}
                                      text ["Point 1"
                                            "Point 2"
                                            "Point 3"]}}]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans title])
   (clerk/row
    {::clerk/width :full}
    (clerk/vl chart)
    (clerk/html
     [:div.text-1xl.max-w-screen-2xl.font-sans
      (bulleted-list text)]))
   (mc-logo)))

(defn title-chart-table-slide [{:keys [title
                                       chart
                                       ds]
                                :or   {title "Title"
                                       chart {}
                                       ds (tc/dataset)}}]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans title])
   (clerk/row
    {::clerk/width :full}
    (clerk/vl chart)
    (clerk/col
     (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name ds)])
     (clerk/table ds)))
   (mc-logo)))
