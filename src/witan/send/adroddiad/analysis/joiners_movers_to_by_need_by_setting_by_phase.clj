(ns witan.send.adroddiad.analysis.joiners-movers-to-by-need-by-setting-by-phase
  (:require
   [witan.send.adroddiad.vega-specs :as vs]
   [witan.send.adroddiad.vega-specs.lines :as vsl]
   [tablecloth.api :as tc]
   [witan.send.adroddiad.transitions :as tr]
   [witan.send :as ws]
   [tech.v3.datatype.functional :as dfn]
   [witan.send.adroddiad.analysis.total-domain :as td]
   [witan.send.domain.academic-years :as day]))

(defn summarise-from-config [{:keys [config-edn pqt-prefix] :as _config}]
  (let [cfg (ws/read-config config-edn)
        numerator-grouping-keys [:calendar-year :setting :need :phase]
        denominator-grouping-keys [:calendar-year :setting :phase]]
    (td/summarise (td/simulation-data-from-config config-edn pqt-prefix)
                  {:numerator-grouping-keys numerator-grouping-keys
                   :denominator-grouping-keys denominator-grouping-keys
                   :historic-transitions-count (-> config-edn
                                                   td/transitions-from-config
                                                   td/historic-ehcp-count)
                   :transform-simulation-f 
                   (fn [sim {:keys [historic-transitions-count]}]
                     (let [census (-> (tc/concat-copying historic-transitions-count sim)
                                      (tc/select-rows #(or 
                                                        (tr/mover? %)
                                                        (tr/joiner? %)))
                                      (tc/map-columns :calendar-year-2 [:calendar-year] #(inc %))
                                      (tc/drop-columns [:calendar-year :setting-1 :need-1 :academic-year-1])
                                      (tc/rename-columns {:calendar-year-2 :calendar-year
                                                          :setting-2 :setting
                                                          :need-2 :need
                                                          :academic-year-2 :academic-year}))
                           denominator (-> census
                                           (tc/map-columns :phase [:academic-year]
                                                           (fn [ncy] ((merge day/school-phase-names {:early-years "Nursery"}) (day/primary-secondary-post16-ncy15+ ncy))))
                                           (tc/group-by denominator-grouping-keys)
                                           (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
                       (-> census
                           (tc/map-columns :phase [:academic-year]
                                           (fn [ncy] ((merge day/school-phase-names {:early-years "Nursery"}) (day/primary-secondary-post16-ncy15+ ncy))))
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

(defn chart [need-by-setting-by-phase-summaries
             {:keys [setting setting-label phase need-colors-and-shapes y-scale y-zero]
              :or   {y-scale false
                     y-zero  true}}]
  (let [label-field :need
        data        (-> (get-in need-by-setting-by-phase-summaries [:total-summary :table])
                        (tc/select-rows #(and (= phase (:phase %))
                                              (= setting (:setting %)))))]
    (vsl/line-and-ribbon-and-rule-plot
     (merge base-chart-spec
            {:data              (-> data
                                    (tc/map-columns :calendar-year [:calendar-year] td/format-calendar-year))
             :chart-title       (format "# EHCP by %s for %s %s" (name label-field) setting-label phase)
             :chart-height      vs/full-height :chart-width vs/two-thirds-width
             :tooltip-formatf   (vsl/number-summary-tooltip {:group label-field :x :calendar-year :tooltip-field :tooltip-column})
             :colors-and-shapes need-colors-and-shapes
             :x                 :calendar-year :x-title     "Census Year at January" :x-format "%b %Y"
             :y-title           "# EHCPs"      :y-zero      y-zero                   :y-scale  y-scale
             :group             label-field    :group-title (clojure.string/capitalize (name label-field))     
             ;; :order-field order-field
             }))))
