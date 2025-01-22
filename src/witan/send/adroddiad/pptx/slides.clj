(ns witan.send.adroddiad.pptx.slides
  (:require [kixi.plugsocket :as kps]
            [witan.send.adroddiad.slides :as was]))

(defn bulleted-list [seq-of-text]
  (->> seq-of-text
       (map #(str "- " %))
       (clojure.string/join "\n\n")))

(defn chart-box [conf]
  "expects a map for a vega-lite chart"
  {:slide-fn :chart-box
   :vega-lite-chart-map (:chart conf)
   :width (cond
            (contains? conf :left-col)
            (/ (- 1920 100) 2)
            :else
            (- 1920 100))
   :y 400
   :x (cond
        (= :chart (:right-col conf))
        960
        :else
        50)})

(defn text-box [conf]
  "expects a seq of strings in a vector"
  {:slide-fn :text-box
   :text (bulleted-list (:text conf))
   :width (cond
            (contains? conf :left-col)
            (/ (- 1920 100) 2)
            :else
            (- 1920 100))
   :x (cond
        (= :text (:right-col conf))
        (/ 1920 2)
        :else
        50)
   :y 400
   :font-size 50.0})

(defn table-box [conf]
  "expects a tablecloth dataset"
  {:slide-fn :table-box
   :ds (:table conf)
   :width (cond
            (contains? conf :left-col)
            (/ (- 1920 100) 2)
            :else
            (- 1920 100))
   :x (cond
        (= :table (:right-col conf))
        1300
        :else
        50)
   :y 370})

(defn box? [col conf]
  (cond
    (= (col conf) :chart)
    (chart-box conf)
    (= (col conf) :table)
    (table-box conf)
    (= (col conf) :text)
    (text-box conf)))

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
   (box? :left-col (assoc conf :left-col (cond
                                           (contains? conf :chart)
                                           :chart
                                           (contains? conf :table)
                                           :table
                                           (contains? conf :text)
                                           :text)))
   {:slide-fn :image-box
    :image was/mc-logo
    :x (- 1920 350)
    :y 900
    :height (partial * 1.5)
    :width (partial * 1.5)}])

(defmethod slide ::was/title-two-columns-slide [conf]
  [{:slide-fn :text-box
    :text (:title conf)
    :width (- 1920 100)
    :x 50 :y 100
    :bold? true
    :font-size 70.0}
   (box? :left-col conf)
   (box? :right-col conf)
   {:slide-fn :image-box
    :image was/mc-logo
    :x (- 1920 350)
    :y 900
    :height (partial * 1.5)
    :width (partial * 1.5)}])
