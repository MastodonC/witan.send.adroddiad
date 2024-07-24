(ns witan.send.adroddiad.analysis.joiners-by-ncy-by-need
  (:require
   [clojure.string :as str]
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.transitions :as tr]
   [witan.send :as ws]
   [tech.v3.datatype.functional :as dfn]
   [witan.send.adroddiad.analysis.total-domain :as td]
   [witan.send.domain.academic-years :as day]))


(defn summarise-from-config [config-edn pqt-prefix]
  (let [cfg (ws/read-config config-edn)
        numerator-grouping-keys [:calendar-year :academic-year :need]
        denominator-grouping-keys [:calendar-year :academic-year]]
    (td/summarise (td/simulation-data-from-config config-edn pqt-prefix)
                  {:numerator-grouping-keys numerator-grouping-keys
                   :denominator-grouping-keys denominator-grouping-keys
                   :historic-transitions-count (-> config-edn
                                                   td/transitions-from-config
                                                   td/historic-ehcp-count)
                   :transform-simulation-f
                   (fn [sim {:keys [historic-transitions-count]}]
                     (let [census (-> (tc/concat-copying historic-transitions-count sim)
                                      (tc/select-rows #(tr/joiner? %))
                                      (tc/map-columns :calendar-year-2 [:calendar-year] #(inc %))
                                      (tc/drop-columns [:calendar-year :setting-1 :need-1 :academic-year-1])
                                      (tc/rename-columns {:calendar-year-2 :calendar-year
                                                          :setting-2 :setting
                                                          :need-2 :need
                                                          :academic-year-2 :academic-year}))
                           denominator (-> census
                                           (tc/group-by denominator-grouping-keys)
                                           (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
                       (-> census
                           (tc/group-by numerator-grouping-keys)
                           (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
                           (td/add-diff :transition-count)
                           (tc/rename-columns
                            {:diff :ehcp-diff
                             :pct-diff :ehcp-pct-diff})
                           (tc/inner-join denominator denominator-grouping-keys)
                           (tc/map-columns :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
                           (tc/order-by numerator-grouping-keys)
                           )))
                   :simulation-count (get-in cfg [:projection-parameters :simulations])})))

(def base-chart-spec
  {:y   :median
   :irl :q1  :iru :q3  :ir-title "50% range"
   :orl :p05 :oru :p95 :or-title "90% range"})

(def half-width 700)
(def third-width 475)

(defn total-summary [joiners-by-ncy-by-need-summaries
                     {:keys [colors-and-shapes y-scale y-zero order-field select-p]
                      :or   {y-scale     false
                             y-zero      true
                             order-field :need
                             select-p (constantly true)}}]
  (let [label-field :need
        data        (-> (get-in joiners-by-ncy-by-need-summaries [:total-summary :table])
                        (tc/select-rows select-p))]
    (vsl/line-and-ribbon-and-rule-plot
     (merge base-chart-spec
            {:data              (-> data
                                    (tc/map-columns :calendar-year [:calendar-year] td/format-calendar-year))
             :chart-title       "# New EHCP by Primary Need"
             :chart-height      vs/full-height :chart-width vs/two-thirds-width
             :tooltip-formatf   (vsl/number-summary-tooltip {:group label-field :x :calendar-year :tooltip-field :tooltip-column})
             :colors-and-shapes colors-and-shapes
             :x                 :calendar-year :x-title     "Census Year at January" :x-format "%b %Y"
             :y-title           "# New EHCPs"  :y-zero      y-zero                   :y-scale  y-scale
             :group             label-field    :group-title "Primary Need"
             :order-field       order-field
             }))))
