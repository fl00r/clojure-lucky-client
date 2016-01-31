(ns lucky-client.backend
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [lucky-client.reactor :as reactor]
            [lucky-client.utils :as utils]))

(def FORCE-STOP-TIMEOUT 5000)
(def ANSWERS-BUFFER 10)
(def INSTANCE-BUFFER 10)

(defn parse-message
  [msg]
  (let [[route [_ & payload]] (split-with #(not= 0 (count %)) msg)]
    (when-not (or (zero? (count route)) (< (count payload) 3))
      (let [[command method body] payload]
        [route (String. command) (String. method) body]))))

(defn instance
  [reactor stopper endpoint options]
  (let [[out in] (reactor/register reactor :ROUTER [endpoint])
        answers (async/chan ANSWERS-BUFFER)
        requests (async/chan INSTANCE-BUFFER)]
    (log/info "Connect to" endpoint)
    (async/go-loop [state {:status :online
                           :in-progress 0
                           :stopper stopper
                           ;; we'll not force stop before `stopper` activated
                           :force-stopper (async/promise-chan)}]
      ;; We may want to complete all requests for two reason:
      ;; - Because endpoint sent us "disconnect", so we should continue working
      ;; and retry connections
      ;; - Because we are stopping. In this case we should send "DISCONNECT" and
      ;; stop working
      (if (and (= :shutdown (:status state)) (zero? (:in-progress state)))
        (do
          (log/debug "Backend complete it work" endpoint)
          (log/debug "Backend is down" endpoint)
          (async/close! requests)
          (async/close! out))
        (async/alt!
          (:force-stopper state)
          ([_]
           (log/warn "Force stop backend" endpoint)
           (async/close! requests)
           (async/close! out))
          (:stopper state)
          ([_]
           (log/debug "Go to shutdown" endpoint)
           (async/>! out ["DISCONNECT"])
           (recur (assoc state
                         ;; we'll never stop again
                         :force-stopper (async/timeout FORCE-STOP-TIMEOUT)
                         :stopper (async/promise-chan)
                         :status :shutdown)))
          in
          ([v]
           (when v
             (if-let [[route command method body] (parse-message v)]
               (case command
                 "REQUEST"
                 (let [answer (async/promise-chan)]
                   (log/debug "Request received" endpoint command method)
                   (async/go
                     (async/>! answers [route (async/<! answer)]))
                   (async/>! requests [answer method body])
                   (recur (assoc state :in-progress (inc (:in-progress state)))))
                 (log/error "Unknown command" command))
               (log/error "Bad message format"))))
          answers
          ([[route value]]
           (async/>! out (concat route ["" "REPLY" value]))
           (recur (assoc state :in-progress (dec (:in-progress state))))))))
    requests))

(def REQUESTS-BUFFER 100)

(defn create
  ([reactor endpoints] (create reactor endpoints {}))
  ([reactor endpoints {:keys []}]
   (let [stopper (async/promise-chan)
         requests (async/merge (map (fn [endpoint]
                                      (instance reactor stopper endpoint {}))
                                    endpoints)
                               REQUESTS-BUFFER)]
     [(fn [] (async/>!! stopper true))
      requests])))

(defrecord Backend [reactor]
  component/Lifecycle
  (start [this]
    (let [[stopper requests] (create (:commands reactor) (:endpoints this))]
      (assoc this
             :requests requests
             :stopper stopper)))
  (stop [this]
    ((:stopper this))
    this))

(defn component
  [endpoints]
  (map->Backend {:endpoints endpoints}))
