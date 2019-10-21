(ns metabase.driver.cubejs
  "Cube.js REST API driver."
  (:require [metabase.driver :as driver]
            [metabase.mbql.util :as mbql.util]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.metric :as metric :refer [Metric]]
            [metabase.driver.cubejs.query-processor :as cubejs.qp]
            [toucan.db :as db]
            [clj-http.client :as client]))

;; Is there any better? https://github.com/metabase/metabase/blob/master/src/metabase/types.clj#L81
(def cubejs-time->metabase-type
  :type/DateTime)

(defn- measure-in-metrics?
  "Checks is the given measure already in the metrics."
  [measure metrics]
  (some (fn [metric] (= (:name measure) (:name metric))) metrics))

(def json-type->base-type
  {:string  :type/Text
   :number  :type/Float
   :time    cubejs-time->metabase-type})

(defn- get-cubes
  "Get all the cubes from the Cube.js REST API."
  [database]
  (let [url    (str (:cubeurl (:details database)) "v1/meta")
        resp   (client/request {:method        :get
                                :url           url
                                :accept        :json
                                :as            :json})
        body   (:body resp)]
    (:cubes body)))

(defn- process-fields
  "Returns the processed fields from the 'measure' or 'dimension' block. Description must be 'measure' or 'dimension'."
  [fields type]
  (for [field fields]
    (merge
     {:name          (:name field)
      :database-type (:type field)
      :field-comment type
      :base-type     (json-type->base-type (keyword (:type field)))}
     (if (= (:type field) cubejs-time->metabase-type) {:special-type :type/CreationTime}))))

(defn- get-field
  "Returns the name and the type for the given field ID."
  [field]
  (let [field (if (= (first field) :field-id) (qp.store/field (second field)))
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
  "Return the name of the given list of field IDs filtered by the type 'measure or dimension),"
  [field-ids type]
  (let [fields   (for [id field-ids] (qp.store/field id))
        filtered (filter #(= (:description %) type) fields)
        names    (for [field filtered] (:name field))]
    names))

(defn- transform-orderby
  "Transform the MBQL order by to a Cube.js order."
  [query])

(defn- aggregation->display-name
  "Get the display name of the metric from an aggregation."
  [aggregation]
  (let [maps (filter map? aggregation)]
    (for [map maps] (:display-name map))))

(defn- get-measures
  "Get the measure fields from a MBQL query."
  [query]
  (concat
    ;; The simplest case is there are a list of fields in the query.
   (let [field-ids   (mbql.util/match (:fields query) [:field-id id] id)
         field-names (get-field-names-by-type field-ids "measure")]
     field-names)
    ;; Another case if we use metrics so Metabase creates the query for us.
    (mbql.util/match query [:aggregation-options _ {:display-name name}] name)))

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
  (let [fields       (mbql.util/match query [:datetime-field [:field-id id] gran] (list id gran))
        named-fields (for [field fields] (list (:name (qp.store/field (first field))) (second field)))]
    (if named-fields (set named-fields))))

(defn- mbql->cubejs
  "Build a valid Cube.js query from the generated MBQL."
  [query]
  (let [measures        (get-measures query)
        dimensions      (get-dimensions query)
        time-dimensions (get-time-dimensions query)
        limit           (:limit query)]
    (merge
     {:measures (if measures measures ("SupplierLeads.leads"))}
     (if dimensions {:dimensions dimensions})
     (if time-dimensions {:timeDimensions (for [td time-dimensions] {:dimension (first td), :granularity (second td)})})
     ;:order (if (:order-by query) ())
     (if limit {:limit limit}))))

;;; Implement Metabase driver functions.

(driver/register! :cubejs)

(defmethod driver/supports? [:cubejs :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :cubejs [_ _]
  ;; TODO: Would be better to implement a simple GET request to check the API availability.
  true)

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
      (if-not (measure-in-metrics? measure metrics)
        (db/insert! Metric
                    :table_id    (:id table)
                    :creator_id  1 ; Use the first (creator, admin) user at the moment.) Any better solution?
                    :name        (:name measure)
                    :description "Created from code"
                    :definition  {:source-table (:id table)
                                  :aggregation  [[:count]]})))
    {:name   (:name cube)
     :schema (:schema cube)
     ; Segments are currently unsupported.
     :fields (set (concat measures dimensions))}))

(defmethod driver/mbql->native :cubejs [_ query]
  (let [base-query  (mbql->cubejs (:query query))]
    {:query base-query
     :mbql? true}))

(defmethod driver/execute-query :cubejs [_ {native-query :native}]
  (cubejs.qp/execute-http-request native-query))