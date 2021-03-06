(ns metabase.driver.cubejs
  "Cube.js REST API driver."
  (:require [clojure.tools.logging :as log]
            [medley.core :as m]
            [toucan.db :as db]
            [metabase.driver :as driver]
            [metabase.driver.cubejs.utils :as cube.utils]
            [metabase.models.metric :as metric :refer [Metric]]
            [metabase.driver.cubejs
             [query-processor :as cubejs.qp]
             [parameters :as parameters]]))

(defn- cubejs-agg->meta-agg
  "Returns the name of the cubejs aggregation in metabase."
  [agg-name]
  (case agg-name
    "number" :count
    "count" :count
    "countDistinct" :count
    "countDistinctApprox" :count
    "sum" :count
    "avg" :count
    "min" :count
    "max" :count
    "runningTotal" :count
    :count))

(defn- measure-in-metrics?
  "Checks is the given measure already in the metrics."
  [metrics measure-name]
  (some #(= (:cube-name (:definition %)) measure-name) metrics))

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
      :field-comment (str type ": "(:description field))
      :description   (:description field)
      :base-type     (cube.utils/cubejs-type->base-type (keyword (:type field)))
      :agg-type      (:aggType field)}
     (if (= (:type field) cube.utils/cubejs-time->metabase-time) {:special-type :type/CreationTime} nil))))

;;; ---------------------------------------------- Metabase functions ------------------------------------------------

(driver/register! :cubejs)

(defmethod driver/supports? [:cubejs :native-parameters] [_ _]                      true)

(defmethod driver/supports? [:cubejs :foreign-keys] [_ _]                           false)
(defmethod driver/supports? [:cubejs :nested-fields] [_ _]                          false)
(defmethod driver/supports? [:cubejs :set-timezone] [_ _]                           false) ; ??
(defmethod driver/supports? [:cubejs :basic-aggregations] [_ _]                     false)
(defmethod driver/supports? [:cubejs :expressions] [_ _]                            false)
(defmethod driver/supports? [:cubejs :expression-aggregations] [_ _]                false)
(defmethod driver/supports? [:cubejs :nested-queries] [_ _]                         false)
(defmethod driver/supports? [:cubejs :binning] [_ _]                                false)
(defmethod driver/supports? [:cubejs :case-sensitivity-string-filter-options] [_ _] false)
(defmethod driver/supports? [:cubejs :left-join] [_ _]                              false)
(defmethod driver/supports? [:cubejs :right-join] [_ _]                             false)
(defmethod driver/supports? [:cubejs :inner-join] [_ _]                             false)
(defmethod driver/supports? [:cubejs :full-join] [_ _]                              false)

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
      (if-not (measure-in-metrics? metrics (:name measure)) ; We can use the `name` of the measure here because it is already untouched (no rename).
        (db/insert! Metric
                    :table_id    (:id table)
                    :creator_id  (let [creator-id (:metrics-creator (:details database))] (if (int? creator-id) creator-id (Integer/parseInt creator-id)))
                    :name        (:name measure)
                    :description (:description measure)
                    :definition  {:source-table (:id table)
                                  :cube-name  (:name measure)
                                  :aggregation  [[(cubejs-agg->meta-agg (:agg-type measure))]]})))
    {:name   (:name cube)
     :schema (:schema cube)
     ;; Segments are currently unsupported.
     ;; Remove the description key from the set of fields then add the `database-position`.
     :fields (set (for [[idx field] (m/indexed (set (concat measures dimensions)))]
                  (assoc (dissoc field :description :agg-type) :database-position (inc idx))))}))

(defmethod driver/mbql->native :cubejs [_ query]
  (log/debug "MBQL:" query)
  (cubejs.qp/mbql->cubejs query))

(defmethod driver/substitute-native-parameters :cubejs
  [driver inner-query]
  (parameters/substitute-native-parameters driver inner-query))

(defmethod driver/execute-reducible-query :cubejs [_ query _ respond]
  (log/debug "Query:" query)
  (cubejs.qp/execute-http-request query respond))