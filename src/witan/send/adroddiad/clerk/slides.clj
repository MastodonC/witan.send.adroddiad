(ns witan.send.adroddiad.clerk.slides
  (:require [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [clojure.java.io :as io]
            [witan.send.adroddiad.slides :as was]))

(defn mc-logo []
  (clerk/html
   {::clerk/width :full}
   [:div.max-w-screen-2xl.font-sans
    [:div.h-full.max-h-full.bottom-0.-right-12.absolute (clerk/image was/mc-logo)]]))

(defn bulleted-list [seq-of-text]
  (reduce #(into %1 [[:li.text-3xl.mb-4.mt-4 %2]]) [:ul.list-disc] seq-of-text))

(defmulti left-box :left-box)

(defmethod left-box ::was/chart [conf]
  (clerk/vl
   {::clerk/width :full}
   (:chart conf)))

(defmethod left-box ::was/text [conf]
  (clerk/html
   {::clerk/width :full}
   [:div.text-1xl.max-w-screen-2xl.font-sans
    (bulleted-list (:text conf))]))

(defmethod left-box ::was/table [conf]
  (let [table (:table conf)]
    (clerk/col
     (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name table)])
     (clerk/table table))))

(defmethod left-box ::was/image [conf]
  (clerk/image (:image conf)))

(defmulti right-box :right-box)

(defmethod right-box ::was/chart [conf]
  (clerk/vl
   {::clerk/width :full}
   (:chart conf)))

(defmethod right-box ::was/text [conf]
  (clerk/html
   {::clerk/width :full}
   [:div.text-1xl.max-w-screen-2xl.font-sans
    (bulleted-list (:text conf))]))

(defmethod right-box ::was/table [conf]
  (let [table (:table conf)]
    (clerk/col
     (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name table)])
     (clerk/table table))))

(defmethod right-box ::was/image [conf]
  (clerk/image (:image conf)))

(defn box-type [conf]
  (cond
    (contains? conf :chart)
    (left-box (assoc conf :left-box ::was/chart))
    (contains? conf :table)
    (left-box (assoc conf :left-box ::was/table))
    (contains? conf :text)
    (left-box (assoc conf :left-box ::was/text))
    (contains? conf :image)
    (left-box (assoc conf :left-box ::was/image))))

(defmulti slide :slide-type)

(defmethod slide ::was/title-slide [conf]
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

(defmethod slide ::was/empty-slide [conf]
  (clerk/fragment
   (clerk/md "")
   (mc-logo)))

(defmethod slide ::was/section-header-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:div
     [:p.text-6xl.font-bold.font-sans (:title conf)]
     [:p.text-4xl.font-bold.font-sans (:sub-title conf)]])
   (mc-logo)))

(defmethod slide ::was/title-body-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:div
     [:p.text-6xl.font-bold.font-sans (:title conf)]
     [:p.text-4xl.font-bold.font-sans (:sub-title conf)]])
   (box-type conf)
   (mc-logo)))

(defmethod slide ::was/title-two-columns-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:div
     [:p.text-6xl.font-bold.font-sans (:title conf)]
     [:p.text-4xl.font-bold.font-sans (:sub-title conf)]])
   (clerk/row
    {::clerk/width :full}
    (left-box conf)
    (right-box conf))
   (mc-logo)))
