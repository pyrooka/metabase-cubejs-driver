(ns metabase.driver.cubejs.query-processor
  (:refer-clojure :exclude [==])
  (:require [flatland.ordered.map :as ordered-map]
            [cheshire.core :as json]
            [metabase.driver.cubejs.utils :as cube.utils]))

(defn- extract-fields
  [rows fields]
  (for [row rows]
    (for [field fields]
      (get row field))))

(defn execute-http-request [native-query]
  (let [query         (if (:query native-query) (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query)))
        resp          (cube.utils/make-request "v1/load" query nil)
        rows          (:data (:body resp))
        cols          (if (:aggregation? native-query) (keys (first rows)))
        result        (if (:aggregation? native-query) {:columns cols :rows (extract-fields rows cols)} {:rows (for [row rows] (into (ordered-map/ordered-map) row))})]
    result))