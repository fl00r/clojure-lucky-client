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
         [requests replies] (reactor/register reactor endpoints)]
     (async/go-loop [mapping {}]
       (async/alt!
         input ([v] (if-let [[command id & tail] v]
                      (case command
                        :request
                        (let [[body answer] tail]
                          (async/>! requests [id "" body])
                          (recur (assoc mapping id answer)))
                        :cancel (recur (dissoc mapping id)))
                      (async/close! requests)))
         replies ([v]
                  (when-let [[id delim body] v]
                    (let [id' (String. id)]
                      (if-let [answer (get mapping id')]
                        (do (async/>! answer body)
                            (recur (dissoc mapping id')))
                        (do (log/warn "Unknown request" {:id id' :endpoints endpoints})
                            (recur mapping))))))))
     input)))

(defn request
  ([client body] (request client body {}))
  ([client body {:keys [timeout]}]
   (let [answer (async/promise-chan)
         id (utils/uuid)]
     (async/go
       (async/>! client [:request id body answer])
       (if timeout
         (async/alt!
           (async/timeout timeout)
           ([_]
            (async/>! client [:cancel id])
            (async/>! answer (Exception. "Timeout"))))))
     answer)))

(defrecord Client [reactor]
  component/Lifecycle
  (start [this] (assoc :ch (create reactor (:endpoints this))))
  (stop [this] (async/close! (:ch this)) this))

(defn component
  [endpoints]
  (map->Client {:endpoints endpoints}))
