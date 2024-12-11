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

(defmulti left-hand-box :left-col)

(defmethod left-hand-box :chart [conf]
  (clerk/vl {::clerk/width :full}
            (:chart conf)))

(defmethod left-hand-box :text [conf]
  (clerk/html
   {::clerk/width :full}
   [:div.text-1xl.max-w-screen-2xl.font-sans
    (bulleted-list (:text conf))]))

(defmethod left-hand-box :table [conf]
  (clerk/col
   (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name (:table conf))])
   (clerk/table (:table conf))))

(defmulti right-hand-box :right-col)

(defmethod right-hand-box :chart [conf]
  (clerk/vl {::clerk/width :full}
            (:chart conf)))

(defmethod right-hand-box :text [conf]
  (clerk/html
   {::clerk/width :full}
   [:div.text-1xl.max-w-screen-2xl.font-sans
    (bulleted-list (:text conf))]))

(defmethod right-hand-box :table [conf]
  (clerk/col
   (clerk/html [:p.text-3xl.font-bold.text-center.font-sans (tc/dataset-name (:table conf))])
   (clerk/table (:table conf))))

(defmulti slide :slide-type)

(defmethod slide :title-slide [conf]
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

(defmethod slide :empty-slide [conf]
  (clerk/fragment
   (clerk/md "")
   (mc-logo)))

(defmethod slide :section-header-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans (:title conf)])
   (mc-logo)))

(defmethod slide :title-body-slide [conf]
  (let [conf' (assoc conf :left-col (-> conf
                                        (dissoc :title :slide-type)
                                        keys
                                        first))]
    (clerk/fragment
     (clerk/html
      {::clerk/width :full}
      [:p.text-6xl.font-bold.font-sans (:title conf')])
     (left-hand-box conf')
     (mc-logo))))

(defmethod slide :title-two-columns-slide [conf]
  (clerk/fragment
   (clerk/html
    {::clerk/width :full}
    [:p.text-6xl.font-bold.font-sans (:title conf)])
   (clerk/row
    {::clerk/width :full}
    (left-hand-box conf)
    (right-hand-box conf))
   (mc-logo)))
