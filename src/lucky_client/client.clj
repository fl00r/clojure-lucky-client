(ns lucky-client.client
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [lucky-client.reactor :as reactor]
            [lucky-client.utils :as utils]))

(defn create
  ([reactor endpoints] (create reactor endpoints {:buffer 100}))
  ([reactor endpoints {:keys [buffer]}]
   (let [input (async/chan buffer)
         [requests replies] (reactor/register reactor :DEALER endpoints)]
     (async/go-loop [mapping {}]
       (async/alt!
         input ([v] (if-let [[command id & tail] v]
                      (case command
                        :request
                        (let [[method body answer timeout-chan timeout-cancel-chan timeout] tail]
                          (if timeout-chan
                            (do (async/alt!
                                  [[requests [id "" "REQUEST" method body]]]
                                  ([_]
                                   (do (async/go
                                         (async/alt!
                                           timeout-cancel-chan :ok
                                           timeout-chan
                                           ([_]
                                            (async/>! input [:cancel id])
                                            (async/>! answer
                                                      (Exception. (str "Client Timeout**: " timeout
                                                                       ", method: " method))))
                                           :priority true))
                                       (recur (assoc mapping id [answer timeout-cancel-chan]))))
                                  timeout-chan
                                  ([_]
                                   (do (async/>! answer
                                                 (Exception. (str "Client Timeout*: " timeout
                                                                  ", method: " method)))
                                       (recur mapping)))))
                            (do (async/>! requests [id "" "REQUEST" method body])
                                (recur (assoc mapping id [answer timeout-cancel-chan])))))
                        :cancel (recur (dissoc mapping id)))
                      (async/close! requests)))
         replies ([v]
                  (when-let [[id delim type body] v]
                    (if (and id delim type body)
                      (let [id' (String. id)]
                        (if-let [[answer timeout-cancel-chan] (get mapping id')]
                          (do (when timeout-cancel-chan (async/>! timeout-cancel-chan true))
                              (async/>! answer (case (String. type)
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
       (try
         (if timeout
           (let [timeout-chan (async/timeout timeout)
                 timeout-cancel-chan (async/promise-chan)]
             (async/alt!
               [[client [:request id method body answer timeout-chan timeout-cancel-chan timeout]]] :ok
               timeout-chan ([_] (async/>! answer
                                           (Exception. (str "Client Timeout: " timeout
                                                            ", method: " method))))))
           (async/>! client [:request id method body answer]))
         (catch Throwable e
           ;; Usually here is Buffer overflow exception
           (async/>! answer e))))
     answer)))

(defrecord Client [reactor]
  component/Lifecycle
  (start [this] (assoc this :ch (create (:commands reactor) (:endpoints this))))
  (stop [this] (async/close! (:ch this)) this))

(defn component
  [endpoints]
  (map->Client {:endpoints endpoints}))
