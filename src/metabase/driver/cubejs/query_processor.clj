(ns metabase.driver.cubejs.query-processor
  (:refer-clojure :exclude [==])
  (:require [metabase.query-processor.store :as qp.store]
            [flatland.ordered.map :as ordered-map]
            [cheshire.core :as json]
            [clj-http.client :as client]))

(defn get-cube-api-url
  "Returns the Cube.js API URL from the config."
  []
  (let [database (qp.store/database)]
    (:cubeurl (:details database))))

(defn extract-fields
  [rows fields]
  (for [row rows]
    (for [field fields]
      (get row field))))

(defn- make-request
  "Make the HTTP request to the Cube.js API and return the response.
   If the response is 200 with a 'Continue wait' error message, try again."
  [url query]
  (loop []
    (let [resp (client/request {:method         :get
                                :url            url
                                :query-params   {"query" query}
                                :accept         :json
                                :as             :json})]
      (if (= (:status resp) 200) ; If the status is OK
      (if (= (:error (:body resp)) "Continue wait") ; check does the response contains "Continue wait" message or not.
        (do
          (Thread/sleep 2000) ; If it contains, wait 2 sec,
          (recur)) ; then retry the query.
        resp)
      resp))))

(defn execute-http-request [native-query]
  (let [url           (str (get-cube-api-url) "v1/load")
        query         (if (:query native-query) (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query)))
        resp          (make-request url query)
        rows          (:data (:body resp))
        cols          (if (:aggregation? native-query) (keys (first rows)))
        result        (if (:aggregation? native-query) {:columns cols :rows (extract-fields rows cols)} {:rows (for [row rows] (into (ordered-map/ordered-map) row))})]
    result))