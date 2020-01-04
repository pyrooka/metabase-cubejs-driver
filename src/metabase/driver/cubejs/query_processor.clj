(ns metabase.driver.cubejs.query-processor
  (:refer-clojure :exclude [==])
  (:require [flatland.ordered.map :as ordered-map]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [metabase.driver.cubejs.utils :as cube.utils]))

(defn- get-types
  "Extract the types for each field in the response from the annotation block."
  [annotation]
  (into {}
        (for [fields (vals annotation)]
          (into {}
                (for [[name info] fields]
                  {name ((keyword (:type info)) cube.utils/json-type->base-type)})))))

(defn- update-row-values
  [row cols]
  (reduce-kv
   (fn [row key val]
     (assoc row key (if (some #(= key %) cols) (cube.utils/string->number val) val))) {} row))

(defn- convert-values
  "Convert the values in the rows to the correct type."
  [rows types]
  ;; Get the number fields from the types.
  (let [num-cols  (map first (filter #(= (second %) :type/Number) types))]
    (map #(update-row-values % num-cols) rows)))

(defn- extract-fields
  "Extract the values from the map and returns them as a list."
  [rows fields]
  (for [row rows]
    (for [field fields]
      (get row field))))

(defn execute-http-request [native-query]
  (log/debug "Native:" native-query)
  (let [query         (if (:query native-query) (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query)))
        resp          (cube.utils/make-request "v1/load" query nil)
        rows          (:data (:body resp))
        annotation    (:annotation (:body resp))
        types         (get-types annotation)
        rows          (convert-values rows types)
        cols          (if (:aggregation? native-query) (keys (first rows)))
        result        (if (:aggregation? native-query) {:columns cols :rows (extract-fields rows cols)} {:rows (for [row rows] (into (ordered-map/ordered-map) row))})]
    result))