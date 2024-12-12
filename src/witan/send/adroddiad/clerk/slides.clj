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

(defn chart-box [chart]
  "expects a map for a veg-lite chart"
  (clerk/vl {::clerk/width :full}
            chart))

(defn text-box [text]
  "expects a seq of strings in a vector"
  (clerk/html
   {::clerk/width :full}
   [:div.text-1xl.max-w-screen-2xl.font-sans
    (bulleted-list text)]))

(defn table-box [table]
  "expects a tablecloth dataset"
  (clerk/col
   (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name table)])
   (clerk/table table)))

(defn box? [pred conf]
  (cond
    (pred :chart)
    (chart-box (:chart conf))
    (pred :table)
    (table-box (:table conf))
    (pred :text)
    (text-box (:text conf))))

(defmulti slide :slide-type)

(defmethod slide ::title-slide [conf]
  (clerk/fragment
   (clerk/html {::clerk/width :full}
               [:div.max-w-screen-2xl.font-sans
                [:p.text-6xl.font-extrabold (:title conf)]
                [:p.text-3xl.font-bold (:work-package conf)]
                [:p.text-3xl.italic (:presentation-date conf)]
                [:p.text-5xl.font-bold.-mb-8.-mt-2 (format "For %s" (:client-name conf))]
                [:p.text-4xl.font-bold.italic "Presented by Mastodon C"]
                [:p.text-1xl "Use arrow keys to navigate and ESC to see an overview."]])
   (mc-logo)))

(defmethod slide ::empty-slide [conf]
  (clerk/fragment
   (clerk/md "")
   (mc-logo)))

(defmethod slide ::section-header-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans (:title conf)])
   (mc-logo)))

(defmethod slide ::title-body-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans (:title conf)])
   (box? (partial contains? conf) conf)
   (mc-logo)))

(defmethod slide ::title-two-columns-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans (:title conf)])
   (clerk/row
    {::clerk/width :full}
    (box? (partial = (:left-col conf)) conf)
    (box? (partial = (:right-col conf)) conf))
   (mc-logo)))
