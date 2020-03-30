(ns metabase.driver.cubejs.query-processor
  (:require [clojure.set :as set]
            [cheshire.core :as json]
            [flatland.ordered.map :as ordered-map]
            [metabase.mbql.util :as mbql.u]
            [metabase.util.date-2 :as u.date]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.cubejs.utils :as cube.utils]))


(def ^:dynamic ^:private *query* nil)

(def ^:private datetime-operators
  ["afterDate", "beforeDate", "inDateRange", "notInDateRange"])

;;; ----------------------------------------------------- common -----------------------------------------------------

(defn- is-datetime-field?
  [[ftype & _] [vtype & _]]
  (or
   (= ftype :datetime-field)
   (= vtype :absolute-datetime)
   (= vtype :relative-datetime)))

(defn- is-datetime-operator?
  [operator]
  (some #(= operator %) datetime-operators))

(defn- get-older
  "Returns the older value."
  [dts1 dts2]
  (let [dt1 (u.date/parse dts1)
        dt2 (u.date/parse dts2)]
    (if (.isBefore dt1 dt2)
      dts1
      dts2)))

(defn- get-newer
  "Returns the newer value."
  [dts1 dts2]
  (let [dt1 (u.date/parse dts1)
        dt2 (u.date/parse dts2)]
    (if (.isBefore dt1 dt2)
      dts2
      dts1)))

(defn- lower-bound [unit t]
  (:start (u.date/range t unit)))

(defn- upper-bound [unit t]
  (:end (u.date/range t unit)))

(defn ^:private mbql-granularity->cubejs-granularity
  [granularity]
  (let [cubejs-granularity (granularity {:default :day
                                         :year    :year
                                         :month   :month
                                         :week    :week
                                         :day     :day
                                         :hour    :hour
                                         :minute  :minute
                                         :second  :second})]
    (if (nil? cubejs-granularity)
      (throw (Exception. (str (name granularity) " granularity not supported by Cube.js")))
      cubejs-granularity)))

(defmulti ^:private ->rvalue
  "Convert something to an 'rvalue`, i.e. a value that could be used in the right-hand side of an assignment expression.
    (let [x 100] ...) ; x is the lvalue; 100 is the rvalue"
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->rvalue nil
  [_]
  nil)

(defmethod ->rvalue Object
  [this]
  (conj [] (str this))) ; in Cube.js filter value is an array

(defmethod ->rvalue :field-id
  [[_ field-id]]
  (:name (qp.store/field field-id)))

(defmethod ->rvalue :aggregation-options [[_ _ ag-names]]
  (:display-name ag-names))

(defmethod ->rvalue :aggregation [[_ value]]
  (if (number? value)
    (->rvalue (nth (:aggregation *query*) value))
    (->rvalue value)))

(defmethod ->rvalue :aggregate-field [[_ index]]
  (->rvalue (nth (:aggregation *query*) index)))

(defmethod ->rvalue :datetime-field
  [[_ field]]
  (->rvalue field))

(defmethod ->rvalue :relative-datetime
  [[_ amount unit]]
  [(u.date/format (u.date/truncate (lower-bound unit (u.date/add unit amount)) unit))
   (u.date/format (u.date/truncate (upper-bound unit (u.date/add unit amount)) unit))])

(defmethod ->rvalue :absolute-datetime
  [[_ t]]
  (->rvalue (u.date/format t)))

(defmethod ->rvalue :value
  [[_ value]]
  (->rvalue value))

;;; ----------------------------------------------------- filter -----------------------------------------------------

(defmulti ^:private parse-filter first)

(defmethod parse-filter nil [] nil)

;; Metabase convert the `set` and `not set` filters to `= nil` `!= nil`.
(defmethod parse-filter :=  [[_ field value]]
  (if-let [rvalue (->rvalue value)]
    (if (is-datetime-field? field value)
      {:member (->rvalue field) :operator "inDateRange" :values (concat rvalue)}
      {:member (->rvalue field) :operator "equals" :values rvalue})
    (parse-filter [:is-null field])))

(defmethod parse-filter :!= [[_ field value]]
  (if-let [rvalue (->rvalue value)]
    {:member (->rvalue field) :operator "notEquals" :values rvalue}
    (parse-filter [:not-null field])))

(defmethod parse-filter :<  [[_ field value]]
  (if (is-datetime-field? field value)
    {:member (->rvalue field) :operator "beforeDate" :values (->rvalue value)}
    {:member (->rvalue field) :operator "lt" :values (->rvalue value)}))
(defmethod parse-filter :>  [[_ field value]]
  (if (is-datetime-field? field value)
    {:member (->rvalue field) :operator "afterDate" :values (->rvalue value)}
    {:member (->rvalue field) :operator "gt" :values (->rvalue value)}))
(defmethod parse-filter :<= [[_ field value]]
  (if (is-datetime-field? field value)
    {:member (->rvalue field) :operator "beforeDate" :values (->rvalue value)}
    {:member (->rvalue field) :operator "lte" :values (->rvalue value)}))
(defmethod parse-filter :>= [[_ field value]]
  (if (is-datetime-field? field value)
    {:member (->rvalue field) :operator "afterDate" :values (->rvalue value)}
    {:member (->rvalue field) :operator "gte" :values (->rvalue value)}))

(defmethod parse-filter :between [[_ field min-val max-val]]
  ;; If the type of the fields is datetime, use inDateRange.
  (if (is-datetime-field? field nil)
    {:member (->rvalue field) :operator "inDateRange" :values (concat (->rvalue min-val) (->rvalue max-val))}
    [{:member (->rvalue field) :operator "gte" :values (->rvalue min-val)}
     {:member (->rvalue field) :operator "lte" :values (->rvalue max-val)}]))

;; Starts/ends-with not implemented in Cube.js yet.
(defmethod parse-filter :starts-with [[_ _]] (throw (Exception. "\"Starts with\" filter is not supported by Cube.js yet")))
(defmethod parse-filter :ends-with   [[_ _]] (throw (Exception. "\"Ends with\" filter is not supported by Cube.js yet")))

(defmethod parse-filter :contains          [[_ field value]] {:member (->rvalue field) :operator "contains" :values (->rvalue value)})
(defmethod parse-filter :does-not-contains [[_ field value]] {:member (->rvalue field) :operator "notContains" :values (->rvalue value)})

(defmethod parse-filter :not-null [[_ field]] {:member (->rvalue field) :operator "set"})
(defmethod parse-filter :is-null [[_ field]] {:member (->rvalue field) :operator "notSet"})

(defmethod parse-filter :and [[_ & args]] (mapv parse-filter args))
(defmethod parse-filter :or  [[_ & args]]
  (let [filters (mapv parse-filter args)]
    (reduce (fn [result filter]
              (update result :values into (:values filter)))
            filters)))

;; Use this code if different fields can be in a single or clause, because this will check and match them.
;; NOTE: the return value is a vector so we have to handle it somehow.
;;(defmethod parse-filter :or  [[_ & args]]
;;  (let [filters (mapv parse-filter args)]
;;    (reduce (fn [result filter]
;;              (if (some #(and (= (:member %) (:member filter)) (= (:operator %) (:operator filter))) result)
;;                (mapv #(if (and (= (:member %) (:member filter)) (= (:operator %) (:operator filter))) (update % :values into (:values filter))) result)
;;                (conj result filter)))
;;            []
;;            filters)))

(defmulti ^:private negate first)

(defmethod negate :default [clause]
  (mbql.u/negate-filter-clause clause))

(defmethod negate :and [[_ & subclauses]] (apply vector :and (map negate subclauses)))

(defmethod negate :or  [[_ & subclauses]] (apply vector :or  (map negate subclauses)))

(defmethod negate :contains [[_ field v opts]] [:does-not-contains field v opts])

(defmethod parse-filter :not [[_ subclause]] (parse-filter (negate subclause)))

(defn- transform-filters
  "Transform the MBQL filters to Cube.js filters."
  [query]
  (let [filter  (:filter query)
        filters (if filter (parse-filter filter) nil)
        raw     (flatten (if (vector? filters) filters (if filters (conj [] filters) nil)))
        result  (filterv #(not (is-datetime-operator? (:operator %))) raw)]
    {:filters result}))

;;; ---------------------------------------------------- order-by ----------------------------------------------------

(defn- handle-order-by
  [{:keys [order-by]}]
    ;; Iterate over the order-by fields.
  {:order (into {} (for [[direction field] order-by]
                     {(->rvalue field) direction}))})

;;; ----------------------------------------------------- limit ------------------------------------------------------

(defn- handle-limit
  [{:keys [limit]}]
  (if-not (nil? limit)
    {:limit limit}
    nil))

;;; ----------------------------------------------------- fields -----------------------------------------------------

(defmulti ^:private ->cubefield
  "Same like the `->rvalue`, but with the value returns the field type and other optional values too."
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->cubefield nil
  [_]
  nil)

(defmethod ->cubefield Object
  [this]
  this)

(defmethod ->cubefield :field-id
  [[_ field-id]]
  (let [field (qp.store/field field-id)]
    {:name (:name field) :type (keyword (:description field))}))

(defmethod ->cubefield :aggregation-options [[_ _ ag-names]]
  (if-let [display-name (:display-name ag-names)]
    {:name display-name :type :measure}
    nil))

(defmethod ->cubefield :datetime-field
  [[_ field granularity]]
  (let [field (->cubefield field)]
    {:name (:name field)
     :type :timeDimension
     :granularity (mbql-granularity->cubejs-granularity granularity)}))


(defn- datetime-filter-optimizer
  "Optimize the datetime filters. If we have more than one filter for a field, merge them into a single `inDateRange` filter."
  [result new]
  (if-not (some #(= (:member %) (:member new)) result)
    (conj result (if (<= (count (:values new)) 2)
                   new
                   (let [first-value   (first (:values new))
                         second-value  (last (:values new))]
                     (merge
                      new
                      {:values [first-value second-value]}))))
    (for [filter result]
      (if-not (= (:member filter) (:member new))
        filter
        (let [old-operator      (:operator filter)
              old-first-value   (first (:values filter))
              old-second-value  (second (:values filter))
              new-operator      (:operator new)
              new-first-value   (first (:values new))
              new-second-value  (second (:values new))]
          (merge
           filter
           (case old-operator
             "beforeDate" (case new-operator
                            "beforeDate"  {:operator "beforeDate"  :values [(get-older old-first-value new-first-value)]}
                            "afterDate"   {:operator "inDateRange" :values [new-first-value, old-first-value]}
                            "inDateRange" {:operator "inDateRange" :values [new-first-value, (get-older old-first-value new-second-value)]})
             "afterDate" (case new-operator
                           "beforeDate"  {:operator "inDateRange" :values [old-first-value, new-first-value]}
                           "afterDate"   {:operator "afterDate" :values [(get-newer old-first-value new-first-value)]}
                           "inDateRange" {:operator "inDateRange" :values [(get-newer old-first-value new-first-value), new-second-value]})
             "inDateRange" (case new-operator
                             "beforeDate"  {:operator "inDateRange" :values [new-first-value, (get-older old-second-value new-first-value)]}
                             "afterDate"   {:operator "inDateRange" :values [(get-newer old-first-value new-first-value), old-second-value]}
                             "inDateRange" {:operator "inDateRange" :values [(get-newer old-first-value new-first-value) (get-older old-second-value new-second-value)]}))))))))

(defn- optimize-datetime-filters
  "If we have more than one filter to the same date field we have to transform them to a single `dateRange` filter."
  [filters]
  (let [datetime-filters   (filterv #(is-datetime-operator? (:operator %)) filters)
        optimized-filters  (reduce datetime-filter-optimizer [] datetime-filters)]
    optimized-filters))

(defn- handle-datetime-filter
  [date-filter]
  (let [date-filters       (if date-filter (parse-filter date-filter) nil)
        raw-filters        (flatten (if (vector? date-filters) date-filters (if date-filters (conj [] date-filters) nil)))
        cube-date-filters  (optimize-datetime-filters raw-filters)
        result-fields      {}]
    (reduce (fn [result-fields, cube-date-filter]
              (assoc result-fields (:member cube-date-filter) {:type :timeDimension :name (:member cube-date-filter) :dateRange (:values cube-date-filter)}))
            result-fields
            cube-date-filters)))

(defn- handle-datetime-fields
  [time-dimensions cube-fields]
  (reduce (fn [time-dimensions new]
            (case (:type new)
              :timeDimension
              (if (contains? time-dimensions (:name new))
                (-> time-dimensions
                    (assoc-in [(:name new) :granularity] (:granularity new))
                    (assoc-in [(:name new) :type] :timeDimensionGran))
                (assoc time-dimensions (:name new) {:type :dimension :name (:name new)}))
              time-dimensions))
          time-dimensions
          cube-fields))


(defn- handle-measures-dimensions-fields
  [cube-fields]
  (let [result {:measures [] :dimensions [] :timeDimensions []}]
    (reduce (fn [result new]
              (case (:type new)
                :measure       (update result :measures #(conj % (:name new)))
                :dimension     (update result :dimensions #(conj % (:name new)))
                result))
            result
            cube-fields)))

(defn- handle-fields
  [{:keys [filter fields aggregation breakout]}]
  (let [time-dimensions-filter (if filter (handle-datetime-filter (concat filter)) nil)
        fields-all       (concat fields aggregation breakout)
        cube-fields      (set (for [field fields-all] (->cubefield field)))
        time-dimensions  (handle-datetime-fields time-dimensions-filter cube-fields)
        result           (handle-measures-dimensions-fields cube-fields)]
    (reduce (fn [result new]
              (case (:type new)
                :timeDimension (update result :timeDimensions #(conj % {:dimension (:name new) :dateRange (:dateRange new)}))
                :timeDimensionGran (update result :timeDimensions #(conj % {:dimension (:name new) :granularity (:granularity new) :dateRange (:dateRange new)}))
                :dimension     (update result :dimensions #(conj % (:name new)))
                result))
            result
            (vals time-dimensions))))

;;; -------------------------------------------------- query build ---------------------------------------------------

(defn mbql->cubejs
  "Build a valid Cube.js query from the generated MBQL."
  [query]
  (binding [*query*  query]
    (let [fields          (handle-fields query)
          filters         (transform-filters query)
          order-by        (handle-order-by query)
          limit           (handle-limit query)]
      (merge fields filters order-by limit))))

;;; ----------------------------------------------- result processing ------------------------------------------------

(defn- parse-number
  "From: https://github.com/metabase/metabase/blob/master/src/metabase/query_processor/middleware/parameters/mbql.clj#L14"
  [value]
  (cond
    (not (string? value)) value
    ;; if the string contains a period then convert to a Double
    (re-find #"\." value)
    (Double/parseDouble value)

    ;; otherwise convert to a Long
    :else
    (Long/parseLong value)))

(defn- get-types
  "Extract the types for each field in the response from the annotation block."
  [annotation]
  (into {}
        (for [fields (vals annotation)]
          (into {}
                (for [[name info] fields]
                  {name ((keyword (:type info)) cube.utils/cubejs-type->base-type)})))))

(defn- update-row-values
  [row cols]
  (reduce-kv
   (fn [row key val]
     (assoc row key (if (some #(= key %) cols) (parse-number val) val))) {} row))

(defn- convert-values
  "Convert the values in the rows to the correct type."
  [rows types]
  ;; Get the number fields from the types.
  (let [num-cols  (map first (filter #(= (second %) :type/Number) types))]
    (map #(update-row-values % num-cols) rows)))

(defn execute-http-request [native-query]
  (if (and (:mbql? native-query) (empty? (:measures (:query native-query))) (empty? (:dimensions (:query native-query))) (empty? (:timeDimensions (:query native-query))))
    {:rows []}
    (let [query         (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query))
          resp          (cube.utils/make-request "v1/load" query nil)
          rows          (:data (:body resp))
          annotation    (:annotation (:body resp))
          types         (get-types annotation)
          rows          (convert-values rows types)
          result        {:rows (for [row rows] (into (ordered-map/ordered-map) (set/rename-keys row (:measure-aliases native-query))))}]
      result)))