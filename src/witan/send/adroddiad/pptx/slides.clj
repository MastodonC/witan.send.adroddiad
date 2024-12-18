(ns witan.send.adroddiad.pptx.slides
  (:require [kixi.plugsocket :as kps]
            [witan.send.adroddiad.slides :as was]))

(defn bulleted-list [seq-of-text]
  (->> seq-of-text
       (map #(str "- " %))
       (clojure.string/join "\n\n")))

(defn chart-box [chart]
  "expects a map for a veg-lite chart"
  {:slide-fn :chart-box
   :vega-lite-chart-map chart
   :y 400})

(defn text-box [text]
  "expects a seq of strings in a vector"
  {:slide-fn :text-box
   :text (bulleted-list text)
   :width (- 1920 100)
   :x 50 :y 400
   :font-size 50.0})

(defn table-box [table]
  "expects a tablecloth dataset"
  {:slide-fn :table-box
   :ds table
   :x 1300
   :y 370})

(defn box? [pred conf]
  (cond
    (pred :chart)
    (chart-box (:chart conf))
    (pred :table)
    (table-box (:table conf))
    (pred :text)
    (text-box (:text conf))))

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

(defmethod slide ::was/empty-slide [conf]
  [])

(defmethod slide ::was/section-header-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :bold? true
    :font-size 80.0
    :x 50 :y 400
    :width (- 1920 100)}
   {:slide-fn :image-box
    :image was/mc-logo
    :x (- 1920 350)
    :y 900
    :height (partial * 1.5)
    :width (partial * 1.5)}])

(defmethod slide ::was/title-body-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :width (- 1920 100)
    :x 50 :y 200
    :bold? true
    :font-size 90.0}
   (box? (partial contains? conf) conf)
   {:slide-fn :image-box
    :image was/mc-logo
    :x (- 1920 350)
    :y 900
    :height (partial * 1.5)
    :width (partial * 1.5)}])
