(ns witan.send.adroddiad.analysis.need-by-setting-by-phase
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
                                      (tr/transitions->census))
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
