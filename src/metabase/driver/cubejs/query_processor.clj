(ns metabase.driver.cubejs.query-processor
  (:refer-clojure :exclude [==])
  (:require [flatland.ordered.map :as ordered-map]
            [clojure.set :as set]
            [cheshire.core :as json]
            [metabase.driver.cubejs.utils :as cube.utils]))


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
  (let [query         (if (:mbql? native-query) (json/generate-string (:query native-query)) (:query native-query))
        resp          (cube.utils/make-request "v1/load" query nil)
        rows          (:data (:body resp))
        annotation    (:annotation (:body resp))
        types         (get-types annotation)
        rows          (convert-values rows types)
        result        {:rows (for [row rows] (into (ordered-map/ordered-map) (set/rename-keys row (:measure-aliases native-query))))}]
    result))