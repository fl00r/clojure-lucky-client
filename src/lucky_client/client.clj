(ns lucky-client.client
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [lucky-client.reactor :as reactor]
            [lucky-client.utils :as utils]))

(defn create
  ([reactor endpoints] (create reactor endpoints {:buffer 10}))
  ([reactor endpoints {:keys [buffer]}]
   (let [input (async/chan buffer)
         [requests replies] (reactor/register reactor :DEALER endpoints)]
     (async/go-loop [mapping {}]
       (async/alt!
         input ([v] (if-let [[command id & tail] v]
                      (case command
                        :request
                        (let [[method body answer start timeout] tail]
                          (if timeout
                            (let [now (System/nanoTime)
                                  timeout* (-> timeout (* 1000) (+ start) (- now) (/ 1000) int)
                                  timeout* (if (pos? timeout*) timeout* 0)]
                              (async/alt!
                                [[requests [id "" "REQUEST" method body]]]
                                ([_] (recur (assoc mapping id answer)))
                                (async/timeout timeout*)
                                ([_]
                                 (async/>! answer (Exception. "Timeout"))
                                 (recur mapping))))
                            (do
                              (async/>! requests [id "" "REQUEST" method body])
                              (recur (assoc mapping id answer))))))
                      (async/close! requests)))
         replies ([v]
                  (when-let [[id delim type body] v]
                    (if (and id delim type body)
                      (let [id' (String. id)]
                        (if-let [answer (get mapping id')]
                          (do (async/>! answer (case (String. type)
                                                 "ERROR" (Exception. (String. body))
                                                 "REPLY" body))
                              (recur (dissoc mapping id')))
                          (do (log/warn "Unknown request" {:id id' :endpoints endpoints})
                              (recur mapping))))
                      (recur mapping))))))
     input)))

(defn request
  ([client method body] (request client method body {}))
  ([client method body {:keys [timeout]}]
   (let [answer (async/promise-chan)
         id (utils/uuid)]
     (async/go
       (if timeout
         (async/alt!
           [[client [:request id method body answer (System/nanoTime) timeout]]] :ok
           (async/timeout timeout)
           ([_]
            (async/>! answer (Exception. "Timeout"))))
         (async/>! client [:request id method body answer])))
     answer)))

(defrecord Client [reactor]
  component/Lifecycle
  (start [this] (assoc this :ch (create (:commands reactor) (:endpoints this))))
  (stop [this] (async/close! (:ch this)) this))

(defn component
  [endpoints]
  (map->Client {:endpoints endpoints}))
