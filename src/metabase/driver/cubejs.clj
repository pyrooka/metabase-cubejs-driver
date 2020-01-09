(ns metabase.driver.cubejs
  "Cube.js REST API driver."
  (:require [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.util.date-2 :as u.date]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.metric :as metric :refer [Metric]]
            [metabase.driver.cubejs.utils :as cube.utils]
            [metabase.driver.cubejs.query-processor :as cubejs.qp]
            [toucan.db :as db]))


(def mbql-granularity->cubejs-granularity
  {:default :day
   :year    :year
   :month   :month
   :week    :week
   :day     :day
   :hour    :hour
   :minute  :minute
   :second  :second})

(defn- measure-in-metrics?
  "Checks is the given measure already in the metrics."
  [metrics measure]
  (some #(= (:name %) measure) metrics))

(defn- get-cubes
  "Get all the cubes from the Cube.js REST API."
  [database]
  (let [resp   (cube.utils/make-request "v1/meta" nil database)
        body   (:body resp)
        cubes  (:cubes body)]
    cubes))

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

(defn- get-field
  "Returns the name and the type for the given field ID."
  [[_ id]]
  (let [field (qp.store/field id)
        name  (:name field)
        type  (:description field)]
    (list name type)))

(defn- get-fields-with-type
  "Returns all the field names with the given type from the given list."
  [fields type]
  (let [filtered (filter (fn [x] (= (second x) type)) fields)
        result   (for [f filtered] (first f))]
    result))

(defn- get-field-names-by-type
  "Return the name of the given list of field IDs filtered by the type 'measure' or 'dimension'"
  [field-ids type]
  (let [fields   (for [id field-ids] (qp.store/field id))
        filtered (filter #(= (:description %) type) fields)
        names    (for [field filtered] (:name field))]
    names))

(defn- transform-orderby
  "Transform the MBQL order by to a Cube.js order."
  [query]
  (let [order-by (:order-by query)]
    ;; Iterate over the order-by fields.
    (into {} (for [[direction [field-type value]] order-by] (;; Get the name of the field based on its type..
                                                             let [fieldname (case field-type
                                                                              :field-id       (first (get-field [field-type value]))
                                                                              :aggregation    (nth (mbql.u/match query [:aggregation-options _ {:display-name name}] name) value)
                                                                              :datetime-field (first (get-field value))
                                                                              nil)]
                                                              {fieldname direction})))))


;;; ------------------------------------------------ field processing ------------------------------------------------

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
(defmethod parse-filter :starts-with [] nil)
(defmethod parse-filter :ends-with   [] nil)

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

(defmethod negate :and [[_ & subclause]] nil)
(defmethod negate :or  [[_ & subclause]] nil)

(defmethod negate :contains [[_ field v opts]] [:does-not-contains field v opts])

(defmethod parse-filter :not [[_ subclause]] (parse-filter (negate subclause)))

(defn- transform-filters
  "Transform the MBQL filters to Cube.js filters."
  [query]
  (let [filter  (:filter query)
        filters (if filter (parse-filter filter))]
    (flatten
     (if (vector? filters)
       filters
       (if filters (conj [] filters))))))


(defn- get-measures
  "Get the measure fields from a MBQL query."
  [query]
  (concat
    ;; The simplest case is there are a list of fields in the query.
   (let [field-ids   (mbql.u/match (:fields query) [:field-id id] id)
         field-names (get-field-names-by-type field-ids "measure")]
     field-names)
   ;; Another case if we use metrics so Metabase creates the query for us.
   (mbql.u/match query [:aggregation-options _ {:display-name name}] name)))

(defn- get-dimensions
  "Get the dimension fields from a MBQL query."
  [query]
  (concat
    ;; The simplest case is there are a list of fields in the query.
   (let [fields       (remove nil? (map get-field (:fields query)))
         aggregations (get-fields-with-type fields "dimension")]
     aggregations)
    ;; Another case if we use metrics so Metabase creates the query for us.
   (let [dimensions (remove nil? (map first (map get-field (:breakout query))))]
     (if dimensions (set dimensions)))))

(defn- get-time-dimensions
  "Get the time dimensions from the MBQL query."
  [query]
  (let [query        (dissoc query :filter :order-by) ; TODO: fix this quick and dirty hack!!!
        fields       (mbql.u/match query [:datetime-field [:field-id id] gran] (list id (get mbql-granularity->cubejs-granularity gran (throw (Exception. (str (name gran) " granularity not supported by Cube.js"))))))
        named-fields (for [field fields] (list (:name (qp.store/field (first field))) (second field)))]
    (if named-fields (set named-fields))))

(defn- mbql->cubejs
  "Build a valid Cube.js query from the generated MBQL."
  [query]
  (let [measures        (get-measures query)
        dimensions      (get-dimensions query)
        time-dimensions (get-time-dimensions query)
        filters         (transform-filters query)
        order-by        (transform-orderby query)
        limit           (:limit query)]
    (merge
     (if (empty? measures) nil {:measures measures})
     (if (empty? dimensions) nil {:dimensions dimensions})
     (if (empty? time-dimensions) nil {:timeDimensions (for [td time-dimensions] {:dimension (first td), :granularity (second td)})})
     (if (empty? filters) nil {:filters filters})
     (if (empty? order-by) nil {:order order-by})
     (if limit {:limit limit}))))

;;; ---------------------------------------------- Metabase functions ------------------------------------------------

(driver/register! :cubejs)

(defmethod driver/supports? [:cubejs :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :cubejs [_ details]
  (if (nil? (get-cubes {:details details})) false true))

(defmethod driver/describe-database :cubejs [_ database]
  {:tables (set (for [cube (get-cubes database)]
                  {:name   (:name cube)
                   :schema (:schema cube)}))})

(defmethod driver/describe-table :cubejs [_ database table]
  (let [cubes      (get-cubes database)
        cube       (first (filter (comp (set (list (:name table))) :name) cubes))
        measures   (process-fields (:measures cube) "measure")
        dimensions (process-fields (:dimensions cube) "dimension")
        metrics    (metric/retrieve-metrics (:id table) :all)]
    (doseq [measure measures]
      (if-not (measure-in-metrics? metrics (:name measure))
        (db/insert! Metric
                    :table_id    (:id table)
                    :creator_id  1 ; Use the first (creator, admin) user at the moment.) Any better solution?
                    :name        (:name measure)
                    :description (:description measure)
                    :definition  {:source-table (:id table)
                                  :aggregation  [[:count]]})))
    {:name   (:name cube)
     :schema (:schema cube)
     ; Segments are currently unsupported.
     ;; Remove the description key from the fields then create a set.
     :fields (set (map #(dissoc % :description) (concat measures dimensions)))}))

(defmethod driver/mbql->native :cubejs [_ query]
  (log/debug "MBQL:" query)
  (let [base-query    (:query query)
        native-query  (mbql->cubejs base-query)]
    {:query            native-query
     :measure-aliases  (into {} (for [[_ _ names] (:aggregation base-query)] {(keyword (:display-name names)) (keyword (:name names))}))
     :mbql?            true}))

(defmethod driver/execute-query :cubejs [_ {native-query :native}]
  (log/debug "Native:" native-query)
  (cubejs.qp/execute-http-request native-query))