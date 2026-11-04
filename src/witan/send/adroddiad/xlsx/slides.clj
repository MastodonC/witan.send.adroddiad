(ns witan.send.adroddiad.xlsx.slides
  (:require [kixi.large :as large]
            [kixi.transcode :as trans]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.slides :as was]))

(defn slides-to-xlsx [slide-coll out-path]
  ;; expects a vector of maps corresponding to clerk slides and
  ;; location for xlsx output
  (let [sheet-specs (mapcat (fn [v] (cond
                                      (contains? (:chart v) :data)
                                      (seq [{::large/data (-> v
                                                              :chart
                                                              :data
                                                              :values
                                                              tc/dataset)
                                             ::large/images [{::large/image (trans/vl-map->bytearray (:chart v))}]
                                             :plot-title (as-> v $
                                                           (:chart $)
                                                           (:title $)
                                                           (cond
                                                             (map? $)
                                                             (:text $)
                                                             :else
                                                             $))}])
                                      (contains? (:chart v) :vconcat)
                                      (map (fn [n] {::large/data (-> v
                                                                     :chart
                                                                     :vconcat
                                                                     (nth n)
                                                                     :data
                                                                     :values
                                                                     tc/dataset)
                                                    ::large/images [{::large/image (trans/vl-map->bytearray (-> v
                                                                                                                :chart
                                                                                                                :vconcat
                                                                                                                (nth n)))}]
                                                    :plot-title (as-> v $
                                                                  (:chart $)
                                                                  (:vconcat $)
                                                                  (nth $ n)
                                                                  (:title $)
                                                                  (cond
                                                                    (map? $)
                                                                    (:text $)
                                                                    :else
                                                                    $))}) (-> v
                                                                              :chart
                                                                              :vconcat
                                                                              count
                                                                              range))
                                      (contains? (:chart v) :hconcat)
                                      (map (fn [n] {::large/data (-> v
                                                                     :chart
                                                                     :hconcat
                                                                     (nth n)
                                                                     :data
                                                                     :values
                                                                     tc/dataset)
                                                    ::large/images [{::large/image (trans/vl-map->bytearray (-> v
                                                                                                                :chart
                                                                                                                :hconcat
                                                                                                                (nth n)))}]
                                                    :plot-title (as-> v $
                                                                  (:chart $)
                                                                  (:hconcat $)
                                                                  (nth $ n)
                                                                  (:title $)
                                                                  (cond
                                                                    (map? $)
                                                                    (:text $)
                                                                    :else
                                                                    $))}) (-> v
                                                                              :chart
                                                                              :hconcat
                                                                              count
                                                                              range))
                                      :else
                                      nil))
                            slide-coll)
        sheet-specs' (map-indexed #(assoc %2 ::large/sheet-name (str (inc %1))) sheet-specs)
        index [{::large/sheet-name "Index"
                ::large/data (tc/dataset {"Sheet name" (map ::large/sheet-name sheet-specs')
                                          "Plot Title" (map :plot-title sheet-specs')})}]]
    (-> index
        (concat sheet-specs')
        large/create-workbook
        (large/save-workbook! out-path))))

(comment

  (def example-slide
    ;; map only requries `:chart` and `:title` keys
    {:title "foo"
     :chart {:data {:values (-> [{"a" "A", "b" 28}, {"a" "B", "b" 55}, {"a" "C", "b" 43},
                                 {"a" "D", "b" 91}, {"a" "E", "b" 81}, {"a" "F", "b" 53},
                                 {"a" "G", "b" 19}, {"a" "H", "b" 87}, {"a" "I", "b" 52}]
                                tc/dataset
                                (tc/rows :as-maps))}
             :mark "bar"
             :encoding {:x {:field "a" :type "nominal" :axis {:labelAngle 0}}
                        :y {:filed "b" :type "quantitative"}}}
     :text ["New slide"]
     :slide-type ::was/title-two-columns-slide
     :left-box ::was/chart
     :right-box ::was/text})

  (slides-to-xlsx [example-slide] "test.xlsx")

  )
