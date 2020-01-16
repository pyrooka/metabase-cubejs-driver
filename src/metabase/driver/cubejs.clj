(ns metabase.driver.cubejs
  "Cube.js REST API driver."
  (:require [clojure.tools.logging :as log]
            [toucan.db :as db]
            [metabase.driver :as driver]
            [metabase.driver.cubejs.utils :as cube.utils]
            [metabase.models.metric :as metric :refer [Metric]]
            [metabase.driver.cubejs.query-processor :as cubejs.qp]))


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

;;; ---------------------------------------------- Metabase functions ------------------------------------------------

(driver/register! :cubejs)

(defmethod driver/supports? [:cubejs :foreign-keys] [_ _]                           false)
(defmethod driver/supports? [:cubejs :nested-fields] [_ _]                          false)
(defmethod driver/supports? [:cubejs :set-timezone] [_ _]                           false) ; ??
(defmethod driver/supports? [:cubejs :basic-aggregations] [_ _]                     false)
(defmethod driver/supports? [:cubejs :expressions] [_ _]                            false)
(defmethod driver/supports? [:cubejs :native-parameters] [_ _]                      false)
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
      (if-not (measure-in-metrics? metrics (:name measure))
        (db/insert! Metric
                    :table_id    (:id table)
                    :creator_id  (:metrics-creator (:details database))
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
        native-query  (cubejs.qp/mbql->cubejs base-query)]
    {:query            native-query
     :measure-aliases  (into {} (for [[_ _ names] (:aggregation base-query)] {(keyword (:display-name names)) (keyword (:name names))}))
     :mbql?            true}))

(defmethod driver/execute-query :cubejs [_ {native-query :native}]
  (log/debug "Native:" native-query)
  (cubejs.qp/execute-http-request native-query))