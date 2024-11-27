(ns witan.send.adroddiad.vega-specs
  (:require
   [clojure2d.color :as color]
   [tablecloth.api :as tc]))

;; Copied from witan.send.adroddiad.clerk.charting-v2 as they will
;; evolve independently


;;; # Sizing
(def full-height 500)
(def full-width 1222)
(def two-thirds-width 822)
(def half-width 850)
(def wide-width 650)
(def prose-width 500)


;;; # Colo(u)rs and shapes
;; Using Tableau 20 palette, excluding the red, ordered with
;; Tableau 10 shades first, then the lighter shades in the same order.
;; With 19 colours and 8 shapes, this gives 152 distinct combinations.

(def colors
  "Tableau 20 palette, excluding the red."
  (map color/format-hex
       [;; tableau 10
        [ 31 119 180 255]
        [255 127  14 255]
        [ 44 160  44 255]
        #_[214  39  40 255]
        [148 103 189 255]
        [140  86  75 255]
        [227 119 194 255]
        [127 127 127 255]
        [188 189  34 255]
        [ 23 190 207 255]
        ;; tableau 20 lighter shades
        [174 199 232 255]
        [255 187 120 255]
        [152 223 138 255]
        [255 152 150 255]
        [197 176 213 255]
        [196 156 148 255]
        [247 182 210 255]
        [199 199 199 255]
        [219 219 141 255]
        [158 218 229 255]]))

(def shapes
  ["circle"                             ; ○
   "square"                             ; □
   "triangle-up"                        ; △
   "triangle-down"                      ; ▽
   "triangle-right"                     ; ▷
   "triangle-left"                      ; ◁
   "cross"                              ; +
   "diamond"                            ; ◇
   ])

(def unicode-shapes
  ["○"                                  ; circle
   "□"                                  ; square
   "△"                                  ; triangle-up
   "▽"                                  ; triangle-down
   "▷"                                  ; triangle-right
   "◁"                                  ; triangle-left
   "+"                                  ; cross
   "◇"                                  ; diamond
   ])

(defn ds->color-and-shape-lookup
  "Given a dataset `ds` with domain values in column `domain-value-column`,
   renames `domain-value-column` to `:domain-value` and
   adds colors and shapes columns."
  ([ds] (ds->color-and-shape-lookup ds :domain-value))
  ([ds domain-value-column]
   (-> ds
       (tc/rename-columns {domain-value-column :domain-value})
       (tc/reorder-columns [:domain-value])
       (tc/add-columns {:color         (cycle colors)
                        :shape         (cycle shapes)
                        :unicode-shape (cycle unicode-shapes)}))))

(defn color-and-shape-lookup [domain]
  (-> (tc/dataset {:domain-value domain})
      (ds->color-and-shape-lookup :domain-value)))

(defn color-map [plot-data color-field color-lookup]
  (let [group-keys (into (sorted-set) (get plot-data color-field))
        filtered-colors (tc/select-rows color-lookup #(group-keys (get % :domain-value)))]
    {:field color-field
     :scale {:range (into [] (:color filtered-colors))
             :domain (into [] (:domain-value filtered-colors))}}))

(defn shape-map [plot-data shape-field shape-lookup]
  (let [group-keys (into (sorted-set) (get plot-data shape-field))
        filtered-shapes (tc/select-rows shape-lookup #(group-keys (get % :domain-value)))]
    {:field shape-field
     :scale {:range (into [] (:shape filtered-shapes))
             :domain (into [] (:domain-value filtered-shapes))}}))

