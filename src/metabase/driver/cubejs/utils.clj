(ns metabase.driver.cubejs.utils
  (:require [metabase.query-processor.store :as qp.store]
            [clj-http.client :as client]))

(defn- get-cube-api-url
  "Returns the Cube.js API URL from the config."
  []
  (:cubeurl (:details (qp.store/database))))

(defn- get-cube-auth-token
  "Returns the authentication token for the Cube.js API."
  []
  (:authtoken (:details (qp.store/database))))

(defn make-request
  "Make the HTTP request to the Cube.js API and return the response.
   If the response is 200 with a 'Continue wait' error message, try again."
  [resource query database]
  (let [api-url    (if (nil? database) (get-cube-api-url) (:cubeurl (:details database)))
        url        (str api-url resource)
        auth-token (if (nil? database) (get-cube-auth-token) (:authtoken (:details database)))]
    (loop []
      (let [resp (client/request {:method         :get
                                  :url            url
                                  :headers        {:authorization auth-token}
                                  :query-params   {"query" query}
                                  :accept         :json
                                  :as             :json})]
        (if (= (:status resp) 200) ; If the status is OK
          (if (= (:error (:body resp)) "Continue wait") ; check does the response contains "Continue wait" message or not.
            (do
              (Thread/sleep 2000) ; If it contains, wait 2 sec,
              (recur)) ; then retry the query.
            resp)
          resp)))))