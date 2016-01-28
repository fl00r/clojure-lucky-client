(ns lucky-client.backend
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [lucky-client.reactor :as reactor]
            [lucky-client.utils :as utils]))

(def CHECK-INTERVAL 1000)

(defn connect
  [reactor endpoint state]
  (async/go
    (let [[out in] (reactor/register reactor [endpoint])]
      (async/>! out ["READY"])
      (assoc state
             :out out
             :in in
             :heartbeat-received (utils/now)
             :heartbeat-sent (utils/now)))))

(defn reconnect
  [reactor endpoint state]
  (async/go
    (log/warn "Reconnect to" endpoint)
    (async/>! (:out state) ["DISCONNECT"])
    (async/close! (:out state))
    (async/<! (connect reactor endpoint state))))

(defn seconds
  [nanoseconds]
  (/ nanoseconds 1000000000))

(defn check
  [reactor endpoint state]
  (async/go
    (let [now (utils/now)]
      (cond
        (> (seconds (- now (:heartbeat-received state))) 10)
        (async/<! (reconnect reactor endpoint state))
        (> (seconds (- now (:heartbeat-sent state))) 3)
        (do (async/>! (:out state) ["HEARTBEAT"])
            (assoc state :heartbeat-sent now))
        :else
        state))))

(def FORCE-STOP-TIMEOUT 5000)
(def ANSWERS-BUFFER 10)

(defn instance
  [reactor stopper requests endpoint options]
  (let [answers (async/chan ANSWERS-BUFFER)]
    (log/info "Connect to" endpoint)
    (async/go-loop [state (->> {:in nil
                                :out nil
                                :status :online
                                :heartbeat-received (utils/now)
                                :heartbeat-sent (utils/now)
                                :in-progress 0
                                :stopper stopper
                                ;; we'll not force stop before `stopper` activated
                                :force-stopper (async/promise-chan)
                                :timer (async/timeout CHECK-INTERVAL)}
                               (connect reactor endpoint)
                               (async/<!))]
      (if (and (= :down (:status state)) (zero? (count (:requests state))))
        (do
          (async/>! (:out state) ["DISCONNECT"])
          (async/close! (:out state)))
        (async/alt!
          (:force-stopper state)
          ([_]
           (async/>! (:out state) ["DISCONNECT"])
           (async/close! (:out state)))
          (:stopper state)
          ([_]
           (async/>! (:out state) ["SHUTDOWN"])
           (recur (assoc state
                         ;; we'll never stop again
                         :force-stopper (async/timeout FORCE-STOP-TIMEOUT)
                         :stopper (async/promise-chan)
                         :status :shutdown)))
          (:in state)
          ([v]
           (if-let [[command & tail] v]
             (case (String. command)
               "REQUEST"
               (let [[id body] tail
                     id' (String. id)
                     answer (async/promise-chan)]
                 (async/go
                   (async/>! answers [id' (async/<! answer)]))
                 (async/>! requests [answer body])
                 (recur (assoc state
                               :in-progress (inc (:in-progress state))
                               :heartbeat-received (utils/now))))
               "HEARTBEAT"
               (recur (assoc state :heartbeat-received (utils/now)))
               "SHUTDOWN"
               (recur (assoc state
                             :state :down
                             :heartbeat-received (utils/now)))
               "DISCONNECT"
               (recur (assoc state
                             :state :down
                             :heartbeat-received (utils/now))))
             (do
               (async/<! (async/timeout 1000))
               (recur (async/<! (reconnect reactor endpoint state))))))
          answers
          ([[id value]]
           (async/>! (:out state) ["REPLY" id value])
           (recur (assoc state
                         :in-progress (dec (:in-progress state))
                         :heartbeat-sent (utils/now))))
          (:timer state)
          ([_] (-> (async/<! (check reactor endpoint state))
                   (assoc :timer (async/timeout CHECK-INTERVAL))
                   (recur))))))))

(def REQUESTS-BUFFER 100)

(defn create
  ([reactor endpoints] (create reactor endpoints {}))
  ([reactor endpoints {:keys []}]
   (let [stopper (async/promise-chan)
         requests (async/chan REQUESTS-BUFFER)]
     ;; FIXME requests should be closed after stop
     (doseq [endpoint endpoints]
       (instance reactor stopper requests endpoint {}))
     [(fn [] (async/>!! stopper true))
      requests])))

(defrecord Backend [reactor]
  component/Lifecycle
  (start [this]
    (let [[stopper requests] (create reactor (:endpoints this))]
      (assoc this
             :requests requests
             :stopper stopper)))
  (stop [this]
    ((:stopper this))
    this))

(defn component
  [endpoints]
  (map->Backend {:endpoints endpoints}))
