(ns witan.send.adroddiad.summary.series
  (:require [kixi.plot.series :as-alias series]))

(defn data->series-and-legend
  "Given vector `[k v]` with `v` a dataset of summary stats, returns
vector `[k m]` with same k and m a map with `:series` and `:legend`
keys values for plotting.

The `config` map must specify the `color-and-shape-map`` and
(optionally) the summary data column to use for the line (default
`:median``).  Summary stats dataset `v` must contain columns: :median
(or `line-y`), :q1 & :q3, :low-95 & :high-95."
  [[k v] {:keys [color-and-shape-map order-key line-y] :as config}]
  (try
    (let [domain-value    (:domain-value k)
          data            (:data v)
          color           (-> domain-value color-and-shape-map :color)
          shape           (-> domain-value color-and-shape-map :shape)
          legend-shape    (-> domain-value color-and-shape-map :legend-shape)
          line-y          (or line-y :median)
          ribbon-1-high-y :q3      ribbon-1-low-y :q1
          ribbon-2-high-y :high-95 ribbon-2-low-y :low-95]
      [k
       {:series
        (series/ds->line-and-double-ribbon
         data
         {:color           color           :shape          shape
          :x               order-key       :line-y         line-y
          :ribbon-1-high-y ribbon-1-high-y :ribbon-1-low-y ribbon-1-low-y
          :ribbon-2-high-y ribbon-2-high-y :ribbon-2-low-y ribbon-2-low-y})
        :legend-entry
        (series/legend-spec domain-value color legend-shape)}])
    (catch Exception e
      (throw (ex-info (format "Failed to create series for %s" k) {:k k :v v :config config} e)))))
