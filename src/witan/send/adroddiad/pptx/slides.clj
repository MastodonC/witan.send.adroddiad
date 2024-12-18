(ns witan.send.adroddiad.pptx.slides
  (:require [kixi.plugsocket :as kps]
            [witan.send.adroddiad.slides :as was]))

(defn bulleted-list [seq-of-text]
  (->> seq-of-text
       (map #(str "- " %))
       (clojure.string/join "\n\n")))

(defmulti slide :slide-type)

(defmethod slide ::was/title-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :x 50 :y 10
    :width (- 1920 100)
    :bold? true
    :font-size 120.0}
   {:slide-fn :text-box
    :text (:work-package conf)
    :x 50 :y 330
    :bold? true
    :font-size 50.0}
   {:slide-fn :text-box
    :text (:presentation-date conf)
    :italic? true
    :x 50 :y 440
    :font-size 50.0}
   {:slide-fn :text-box
    :text (format "For %s" (:client-name conf))
    :width (- 1920 100)
    :x 50 :y 530
    :bold? true
    :font-size 80.0}
   {:slide-fn :text-box
    :text "Presented by Mastodon C"
    :width (- 1920 100)
    :x 50 :y 650
    :bold? true
    :italic? true
    :font-size 50.0}
   {:slide-fn :image-box
    :image was/mc-logo
    :x (- 1920 350)
    :y 900
    :height (partial * 1.5)
    :width (partial * 1.5)}])
