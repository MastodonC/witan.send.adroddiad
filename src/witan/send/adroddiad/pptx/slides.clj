(ns witan.send.adroddiad.pptx.slides
  (:require [clojure.java.io :as io]
            [kixi.plugsocket :as kps]
            [witan.send.adroddiad.slides :as was]))

(defn bulleted-list [seq-of-text]
  (->> seq-of-text
       (map #(str "- " %))
       (clojure.string/join "\n\n")))

(def slide-length 1920)

(def slide-height 1080)

(def margin 50)

(def margins (* margin 2))

(def slide-minus-margins (- slide-length margins))

(def slide-mid-point (/ slide-length 2))

(def mc-logo-map
  {:slide-fn :image-box
   :image was/mc-logo
   :x (- 1920 350)
   :y 900
   :height (partial * 1.5)
   :width (partial * 1.5)})

(defn constrain-width [conf]
  (cond
    (and (contains? conf :left-box)
         (contains? conf :right-box))
    slide-mid-point
    :else
    slide-minus-margins))

(defmulti left-box :left-box)

(defmethod left-box ::was/chart [conf]
  {:slide-fn :chart-box
   :vega-lite-chart-map (:chart conf)
   :width (constrain-width conf)
   :y 400
   :x margin})

(defmethod left-box ::was/text [conf]
  {:slide-fn :text-box
   :text (bulleted-list (:text conf))
   :width (constrain-width conf)
   :x margin
   :y 400
   :font-size 50.0})

(defmethod left-box ::was/table [conf]
  {:slide-fn :table-box
   :ds (:table conf)
   :width (constrain-width conf)
   :x margin
   :y 370})

(defmethod left-box ::was/image [conf]
  {:slide-fn :image-box
   :image (io/file (:image conf))
   :width (constrain-width conf)
   :x margin
   :y 400})

(defmulti right-box :right-box)

(defmethod right-box ::was/chart [conf]
  {:slide-fn :chart-box
   :vega-lite-chart-map (:chart conf)
   :width (constrain-width conf)
   :y 400
   :x slide-mid-point})

(defmethod right-box ::was/text [conf]
  {:slide-fn :text-box
   :text (bulleted-list (:text conf))
   :width (constrain-width conf)
   :x slide-mid-point
   :y 400
   :font-size 50.0})

(defmethod right-box ::was/table [conf]
  {:slide-fn :table-box
   :ds (:table conf)
   :width (constrain-width conf)
   :x slide-mid-point
   :y 370})

(defmethod right-box ::was/image [conf]
  {:slide-fn :image-box
   :image (io/file (:image conf))
   :width (constrain-width conf)
   :x slide-mid-point
   :y 400})

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
  [{:slide-fn :text-box
    :text (:title conf)
    :x margin :y 10
    :width slide-minus-margins
    :bold? true
    :font-size 120.0}
   {:slide-fn :text-box
    :text (:work-package conf)
    :x margin :y 330
    :bold? true
    :font-size 50.0}
   {:slide-fn :text-box
    :text (:presentation-date conf)
    :italic? true
    :x margin :y 440
    :font-size 50.0}
   {:slide-fn :text-box
    :text (format "For %s" (:client-name conf))
    :width slide-minus-margins
    :x margin :y 530
    :bold? true
    :font-size 80.0}
   {:slide-fn :text-box
    :text "Presented by Mastodon C"
    :width slide-minus-margins
    :x margin :y 650
    :bold? true
    :italic? true
    :font-size 50.0}
   mc-logo-map])

(defmethod slide ::was/empty-slide [conf]
  [])

(defmethod slide ::was/section-header-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :bold? true
    :font-size 80.0
    :x margin :y 400
    :width slide-minus-margins}
   mc-logo-map])

(defmethod slide ::was/title-body-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :width slide-minus-margins
    :x margin :y 200
    :bold? true
    :font-size 90.0}
   (box-type conf)
   mc-logo-map])

(defmethod slide ::was/title-two-columns-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :width slide-minus-margins
    :x margin :y 100
    :bold? true
    :font-size 70.0}
   (left-box conf)
   (right-box conf)
   mc-logo-map])
