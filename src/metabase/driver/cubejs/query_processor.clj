(ns metabase.driver.cubejs.query-processor
  (:require [clojure.set :as set]
            [cheshire.core :as json]
            [flatland.ordered.map :as ordered-map]
            [metabase.mbql.util :as mbql.u]
            [metabase.util.date-2 :as u.date]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.cubejs.utils :as cube.utils]))


(def ^:dynamic ^:private *query* nil)

;;; ----------------------------------------------------- common -----------------------------------------------------

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

(defn- process-fields
  "Returns the processed fields from the 'measure' or 'dimension' block. Description must be 'measure' or 'dimension'."
  [fields type]
  (for [field fields]
    (merge
     {:name          (:name field)
      :database-type (:type field)
      :field-comment type
      :description   (:description field)
      :base-type     (cube.utils/cubejs-type->base-type (keyword (:type field)))}
     (if (= (:type field) cube.utils/cubejs-time->metabase-time) {:special-type :type/CreationTime}))))

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

(defmethod ->rvalue :aggregate-field [[_ index]]
  (->rvalue (nth (:aggregation *query*) index)))

(defmethod ->rvalue :datetime-field
  [[_ field]]
  (->rvalue field))

(defmethod ->rvalue :relative-datetime
  [[_ amount unit]]
  (->rvalue [:absolute-datetime (u.date/add (or unit :day) amount) unit]))

(defmethod ->rvalue :absolute-datetime
  [[_ t]]
  (->rvalue (u.date/format t)))

(defmethod ->rvalue :value
  [[_ value]]
  (->rvalue value))

;;; ----------------------------------------------------- filter -----------------------------------------------------

(defmulti ^:private parse-filter first)

(defmethod parse-filter nil [] nil)

(defmethod parse-filter :=  [[_ field value]]
  (if-let [rvalue (->rvalue value)]
    {:member (->rvalue field) :operator "equals" :values rvalue}
    (parse-filter [:is-null field])))

(defmethod parse-filter :!= [[_ field value]]
  (if-let [rvalue (->rvalue value)]
    {:member (->rvalue field) :operator "notEquals" :values rvalue}
    (parse-filter [:not-null field])))

(defmethod parse-filter :<  [[_ field value]] {:member (->rvalue field) :operator "lt" :values (->rvalue value)})
(defmethod parse-filter :>  [[_ field value]] {:member (->rvalue field) :operator "gt" :values (->rvalue value)})
(defmethod parse-filter :<= [[_ field value]] {:member (->rvalue field) :operator "lte" :values (->rvalue value)})
(defmethod parse-filter :>= [[_ field value]] {:member (->rvalue field) :operator "gte" :values (->rvalue value)})

(defmethod parse-filter :between [[_ field min-val max-val]] [{:member (->rvalue field) :operator "gte" :values (->rvalue min-val)}
                                                              {:member (->rvalue field) :operator "lte" :values (->rvalue max-val)}])

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
        filters (if filter (parse-filter filter))
        result  (flatten (if (vector? filters) filters (if filters (conj [] filters))))]
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

(defn- handle-fields
  [{:keys [fields aggregation breakout]}]
  (let [fields-all   (concat fields aggregation breakout)
        cube-fields  (set (for [field fields-all] (->cubefield field)))
        result       {:measures [] :dimensions [] :timeDimensions []}]
    (reduce (fn [result new]
              (case (:type new)
                :measure       (update result :measures #(conj % (:name new)))
                :dimension     (update result :dimensions #(conj % (:name new)))
                :timeDimension (update result :timeDimensions #(conj % {:dimension (:name new) :granularity (:granularity new)}))
                nil))
            result
            cube-fields)))

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

(defn- string->number
  "From: https://github.com/metabase/metabase/blob/master/src/metabase/query_processor/middleware/parameters/mbql.clj#L14"
  [value]
  (cond
    (not (string? value)) nil
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
     (assoc row key (if (some #(= key %) cols) (string->number val) val))) {} row))

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