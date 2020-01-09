(ns metabase.driver.cubejs.utils
  (:require [clojure.tools.logging :as log]
            [metabase.query-processor.store :as qp.store]
            [clj-http.client :as client]))


;; Is there any better? https://github.com/metabase/metabase/blob/master/src/metabase/types.clj#L81
(def cubejs-time->metabase-time
  :type/DateTime)

(def cubejs-type->base-type
  {:string  :type/Text
   :number  :type/Number
   :boolean :type/Boolean
   :time    cubejs-time->metabase-time})

(defn string->number
  "Convert the string to a number."
  [string]
  (let [val (read-string string)]
    (if (number? val) val nil)))

(defn- get-cube-api-url
  "Returns the Cube.js API URL from the config."
  []
  (:cubeurl (:details (qp.store/database))))

(defn- check-url-ending-slash
  "If the last character of the URL is not a '/' append one."
  [url]
  (if-not (= (last url) \/) (str url "/") url))

(defn- get-cube-auth-token
  "Returns the authentication token for the Cube.js API."
  []
  (:authtoken (:details (qp.store/database))))

(defn make-request
  "Make the HTTP request to the Cube.js API and return the response.
   If the response is 200 with a 'Continue wait' error message, try again."
  [resource query database]
  (let [api-url    (if (nil? database) (get-cube-api-url) (:cubeurl (:details database)))
        url        (str (check-url-ending-slash api-url) resource)
        auth-token (if (nil? database) (get-cube-auth-token) (:authtoken (:details database)))]
    (loop []
      (log/debug "Request:" url auth-token query)
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