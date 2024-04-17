(ns witan.send.adroddiad.summary-v2
  (:require
   [clojure.math :as maths]
   [fastmath.core :as m]
   [ham-fisted.api :as hf]
   [ham-fisted.reduce :as hf-reduce]
   [net.cgrand.xforms :as x]
   [tablecloth.api :as tc]
   [tech.v3.dataset.reductions :as ds-reduce]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.datatype.gradient :as gradient]
   [witan.send.adroddiad.transitions :as tr]))

;; FIXME: This should be merged with summarise-simulations in the rfn and merge-fn for v3
(defn transform-simulations 
  "This takes a simulation transformation fn and retuns a vector of
  transformed simulation datasets with the maximum simulation number
  as a piece of metadata"
  [{:keys [simulation-transform-fn]} ds-vec]
  (hf-reduce/preduce
   ;; init-val-fn
   (fn [] (vary-meta [] assoc :simulation -1))
   ;; rfn
   (fn [acc sim]
     (let [simulation-number (-> sim :simulation first)
           max-sim-number (max simulation-number (-> acc meta :simulation))]
       (vary-meta
        (conj acc (simulation-transform-fn sim))
        assoc :simulation max-sim-number)))
   ;; merge-fn
   (fn [m1 m2]
     (let [max-sim-number (max (-> m1 meta :simulation)
                               (-> m2 meta :simulation))]
       (vary-meta
        (into m1 m2)
        assoc :simulation max-sim-number)))
   {:min-n 10 :ordered? false}
   (hf/vec ds-vec)))

(defn summarise-simulations [{:keys [observation-key value-key finaliser-fn]} ds-vec]
  (-> (ds-reduce/group-by-column-agg
       observation-key
       {:observations (ds-reduce/reducer
                       value-key
                       ;; init-val
                       (fn [] [])
                       ;; rfn
                       (fn [xs x] (conj xs x))
                       ;; merge-fn
                       (fn [v1 v2] (into v1 v2))
                       ;; finaliser
                       (fn [xs] (if finaliser-fn
                                  (finaliser-fn (meta ds-vec) xs) ; this needs the metadata from ds-vec
                                  {:observations xs})))}
       ds-vec)
      (tc/separate-column :observations :infer identity)
      (vary-meta assoc :simulation (-> ds-vec meta :simulation))))

(defn quantile-for-sorted
  "Return `q`th quantile (q*100 th percentile) for a _complete_ & _sorted_
   vector of `n` values `padded-sorted-xs`, linearly interpolating where
   the quantile lies between observations.

   Using algorithm from [org.apache.commons.math3.stat.descriptive.rank percentile class](https://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/descriptive/rank/Percentile.html). "
  [n q padded-sorted-xs]
  ;; - Algorithm re-written in terms of (zero based) index
  ;;   (rather than 1 based position) and quantile rather than percentile:
  ;; - Let n be the length of the (sorted) array (after zero padding)
  ;;   and 0 <= q <= 1 be the desired quantile.
  ;; - If n = 1 return the unique array element (regardless of the value of q);
  ;;   otherwise
  ;; - Compute the estimated quantile index idx = q * (n + 1) -1
  ;;   and the difference d between idx and floor(idx)
  ;;   (i.e. the fractional part of idx).
  ;; - If idx < 0 return the smallest element in the array.
  ;; - Else if idx >= (n-1) return the largest element in the array.
  ;; - Else let lower be the element at index floor(idx) in the sorted array
  ;;   and let upper be the next element in the array.
  ;;   Return lower + d * (upper - lower)
  (if (= 1 n)
    (first padded-sorted-xs)
    (let [idx (dec (* q (inc n)))]
      (cond 
        (< idx 0) (first padded-sorted-xs)
        (<= (dec n) idx) (last padded-sorted-xs)
        :else (let [idx-floor (maths/floor idx)
                    d (- idx idx-floor)
                    lower-val (nth padded-sorted-xs idx-floor)
                    upper-val (nth padded-sorted-xs (inc idx-floor))]
                (+ lower-val (* d (- upper-val lower-val))))))))

(defn completed-simulation-stats [m xs]
  ;; sort xs before passing into quantile  
  ;; pad with 0s at the beginning based on the difference between the length of xs and the num-sims
  (let [observations     (count xs)
        simulation-count (-> m :simulation inc)
        missing-0s       (vec (repeat 
                               (- simulation-count observations)
                               0))
        padded-sorted-xs (x/into missing-0s
                                 (x/sort)
                                 xs)]
    {:min              (quantile-for-sorted simulation-count (/   0 100) padded-sorted-xs)
     :p05              (quantile-for-sorted simulation-count (/   5 100) padded-sorted-xs)
     :q1               (quantile-for-sorted simulation-count (/  25 100) padded-sorted-xs)
     :median           (quantile-for-sorted simulation-count (/  50 100) padded-sorted-xs)
     :q3               (quantile-for-sorted simulation-count (/  75 100) padded-sorted-xs)
     :p95              (quantile-for-sorted simulation-count (/  95 100) padded-sorted-xs)
     :max              (quantile-for-sorted simulation-count (/ 100 100) padded-sorted-xs)
     :mean             (double (/ (reduce + xs) simulation-count))
     :simulation-count simulation-count
     :observations     observations}))

(defn summarise-census-by-keys [simulation-ds-seq {:keys [historical-transitions-counts ks extra-simulation-transform-f finalise-f]
                                                   :or {finalise-f identity
                                                        extra-simulation-transform-f identity}}]
  (as-> simulation-ds-seq $
    (transform-simulations
     {:simulation-transform-fn
      (fn [sim]
        (-> sim
            (tc/concat-copying historical-transitions-counts)
            (tr/transitions->census)
            (tc/drop-rows #(< 20 (% :academic-year)))
            extra-simulation-transform-f
            (tc/group-by ks)
            (tc/aggregate {:transition-count #(dfn/sum (:transition-count %))})
            (tc/order-by ks)))}
     $)
    (summarise-simulations
     {:observation-key ks
      :value-key :transition-count
      :finaliser-fn completed-simulation-stats}
     $)
    (apply (partial tc/complete $) ks)
    (tc/replace-missing $ [:min :p05 :q1 :median :q3 :p95 :max :mean :observations :simulation-count] :value 0)
    (tc/order-by $ ks)
    (finalise-f $)))

(defn add-diff [ds value-col]
  (let [ds' (tc/order-by ds [:calendar-year])
        diff (gradient/diff1d (value-col ds'))
        values (value-col ds')
        pct-diff (sequence
                  (map (fn [d m] (m/approx (* 100 (double (/ d m))))))
                  diff values)]
    (-> ds'
        (tc/add-column :diff (into [0] diff))
        (tc/add-column :pct-diff (into [0] pct-diff)))))

(defn value-at [ds predicate key]
  (-> ds
      (tc/select-rows predicate)
      (tc/rows :as-maps)
      first
      key))
