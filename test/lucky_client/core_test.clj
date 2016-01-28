(ns lucky-client.core-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [lucky-client.core :refer :all]
            [lucky-client.reactor :as reactor]
            [lucky-client.backend :as backend]
            [lucky-client.client :as client]
            [lucky-client.zmq :as zmq]))

(def ^:dynamic *reactor*)

(defn with-reactor
  [f]
  (let [zmq (zmq/create)
        reactor (reactor/create zmq)]
    (try
      (binding [*reactor* reactor]
        (f))
      (finally
        (async/close! reactor)
        (.term zmq)))))

(use-fixtures :each with-reactor)

(defmacro with-timeout
  [ch]
  `(async/alt!!
     (async/timeout 1000) (throw (Exception. "Timeout"))
     ~ch ([v#] v#)))

(deftest basic-test
  (let [[backend-stopper requests] (backend/create *reactor* ["tcp://0.0.0.0:6001"])
        client (client/create *reactor* ["tcp://0.0.0.0:6000"])]
    (try
      (async/go-loop []
        (when-let [[answer body] (async/<! requests)]
          (async/>! answer body)
          (recur)))
      (Thread/sleep 500)
      (let [res (client/request client "Hey, whatzuuup?")]
        (is (= "Hey, whatzuuup?" (String. (with-timeout res)))))
      (let [res (client/request client "WHAT?")]
        (is (= "WHAT?" (String. (with-timeout res)))))
      (finally
        (backend-stopper)))))

(defn start-backend
  []
  (let [zmq (zmq/create)
        reactor (reactor/create zmq)
        [backend-stopper requests] (backend/create reactor ["tcp://0.0.0.0:6001"])]
    (async/go-loop []
      (when-let [[answer body] (async/<! requests)]
        (async/>! answer body)
        (recur)))
    (fn []
      (backend-stopper)
      (async/close! reactor)
      (.term zmq))))
