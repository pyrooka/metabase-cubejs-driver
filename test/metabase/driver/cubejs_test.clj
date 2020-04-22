(ns metabase.driver.cubejs-test
  "Test for the Cube.js driver."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.cubejs.query-processor :as cubejs.qp]))

(deftest mbql->cubejs-test
  (testing "MBQL to Cube.js query"
    (qp.store/do-with-store
     (let [test-data (with-open [r (io/reader "/driver/metabase-cubejs-driver/test/metabase/driver/test_data.json")]
                       (json/parse-stream r true))]
       (qp.store/store-field! {:name "Characters.numberOfUsers" :description "asd"})
       (qp.store/store-field! {:name "Characters.uniqueFirstNames" :description "asd"})
       (qp.store/store-field! {:name "Characters.active" :description "asd"})
       (qp.store/store-field! {:name "Characters.countrycode" :description "asd"})
       (qp.store/store-field! {:name "Characters.lastname" :description "asd"})
       (qp.store/store-field! {:name "Characters.firstname" :description "asd"})
       (qp.store/store-field! {:name "Characters.birth" :description "asd"})
       (doseq [[test-name mb-query]
               [[:simple_filters {:type :query, :query {:source-table 6, :filter [:and [:!= [:field-id 42] [:value "Hagrid" {:base_type :type/Text, :special_type :type/Category, :database_type "string"}]] [:!= [:field-id 42] [:value "Dumbledore" {:base_type :type/Text, :special_type :type/Category, :database_type "string"}]] [:!= [:field-id 42] [:value "Malfoy" {:base_type :type/Text, :special_type :type/Category, :database_type "string"}]] [:!= [:field-id 42] [:value "Weasley" {:base_type :type/Text, :special_type :type/Category, :database_type "string"}]] [:!= [:field-id 40] [:value "Albus" {:base_type :type/Text, :special_type nil, :database_type "string"}]] [:!= [:field-id 40] [:value "Draco" {:base_type :type/Text, :special_type nil, :database_type "string"}]] [:!= [:field-id 42] [:value "Granger" {:base_type :type/Text, :special_type :type/Category, :database_type "string"}]] [:!= [:field-id 40] [:value "Rubeus" {:base_type :type/Text, :special_type nil, :database_type "string"}]] [:!= [:field-id 40] [:value "Hermione" {:base_type :type/Text, :special_type nil, :database_type "string"}]]], :fields [[:field-id 39] [:datetime-field [:field-id 43] :default] [:field-id 37] [:field-id 40] [:field-id 42] [:field-id 38] [:field-id 41]], :limit 2000}, :database 2, :middleware {:add-default-userland-constraints? true}, :info {:executed-by 1, :context :ad-hoc, :nested? false, :constraints {:max-results 10000, :max-results-bare-rows 2000}}}]]]
         (testing test-name
           (is (=
                (cubejs.qp/mbql->cubejs (:query mb-query))
                (get test-data test-name)))))))))