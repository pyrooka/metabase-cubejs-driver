(ns metabase.driver.cubejs.query-processor
  (:refer-clojure :exclude [==])
  (:require [clojure.walk :as walk]
            [metabase.query-processor.store :as qp.store]
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

(defn execute-http-request [native-query]
  (let [url           (str (get-cube-api-url) "v1/load")
        query         (if (:query native-query) (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query)))
        result        (client/request {:method         :get
                                       :url            url
                                       :query-params   {"query" query}
                                       :headers        (:headers native-query)
                                       :accept         :json
                                       :as             :json})
        rows          (:data (:body result))
        fields        (or (:fields (:result native-query)) (keys (first rows)))]
    {:columns fields
     :rows    (extract-fields rows fields)}))