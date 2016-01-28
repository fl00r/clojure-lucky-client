(ns lucky-client.reactor
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [lucky-client.utils :as utils]
            [clojure.tools.logging :as log])
  (:import [org.zeromq ZMQ ZMQ$Poller]))

;; Reactor consists of two loops:
;; - IO loop. This is the only place where we touch external ZMQ-sockets, it
;; starts in async's `thread` pool. It executes zmq_poll in loop. It uses
;; internal inproc socket (for reads) and async's channel (for rites) to
;; communicate with the second loop
;; - command-loop. Its role is communication with the rest world via async's channel.
;;
;; Reactor exposes itself as two channels for each socket: channel of requests
;; and channel of replies.

(defn send-all
  [socket values]
  (let [len (count values)]
    (loop [i 0]
      (cond
        (= len i) nil
        (= len (inc i)) (do (.send socket (nth values i))
                            (recur (inc i)))
        :else (do (.sendMore socket (nth values i))
                  (recur (inc i)))))))

(defn recv-all
  [socket]
  (loop [acc ()]
    (let [data (.recv socket ZMQ/DONTWAIT)]
      (when data
        (let [acc' (cons data acc)]
          (if (.hasReceiveMore socket)
            (recur acc')
            (reverse acc')))))))

(defn io-loop
  [{:keys [zmq stopper id commands internal]}]
  (async/thread
    (let [;; internal (doto (.socket zmq ZMQ/PULL)
          ;;            (.setLinger 0)
          ;;            (.bind (str "inproc://lucky_internal_" id)))
          poller (doto (.poller zmq)
                   (.setTimeout 100)
                   (.register internal ZMQ$Poller/POLLIN))
          external-handler
          (fn [id data]
            (async/>!! commands [:reply id data]))
          handle-socket-f
          (fn [socket handler]
            (loop []
              (when-let [data (recv-all socket)]
                (handler data)
                (recur))))
          handle-socket-reduced-f
          (fn [socket val handler]
            (loop [res val]
              (if-let [data (recv-all socket)]
                (recur (handler res data))
                res)))
          internal-handler
          (fn [poller sockets [command socket-id & tail]]
            (let [socket-id' (String. socket-id)]
              (case (String. command)
                "CONNECT"
                (try
                  (let [socket (doto (.socket zmq ZMQ/DEALER)
                                 (.setLinger 1000))]
                    (doseq [endpoint tail]
                      (.connect socket (String. endpoint)))
                    (.register poller socket)
                    (cons [socket-id' socket] sockets))
                  (catch Exception e
                    (log/error e "Can't connect")
                    (async/>!! commands [:error socket-id' e])
                    sockets))
                "DISCONNECT"
                (if-let [socket (utils/alist-find-by-k sockets socket-id')]
                  (do
                    (.unregister poller socket)
                    (.close socket)
                    (utils/alist-remove sockets socket-id'))
                  (do (log/error "Unknown socket in IO loop (from internal)")
                      sockets))
                "REQUEST"
                (if-let [socket (utils/alist-find-by-k sockets socket-id')]
                  (do (send-all socket tail)
                      sockets)
                  (do (log/error "Unknown socket in IO loop (from internal)")
                      sockets)))))
          handle-item-f
          (fn [sockets item]
            (let [socket (.getSocket item)]
              (if-let [id (utils/alist-find-by-v sockets socket)]
                (handle-socket-f socket (partial external-handler id))
                (log/error "Unknown socket in IO loop (from external)"))))]
      (try
        (loop [sockets ()]
          (if-not (async/poll! stopper)
            (do (.poll poller)
                ;; (log/info "---INTERNAL STATUS" (-> poller (.getItem 0) (.isReadable)))
                (doseq [i (range 1 (.getNext poller))]
                  (let [item (.getItem poller i)]
                    (when (.isReadable item)
                      (try
                        (handle-item-f sockets item)
                        (catch Exception e
                          (log/error e "Error while handling socket"))))))
                (if (-> poller (.getItem 0) (.isReadable))
                  (let [res (try
                              (handle-socket-reduced-f internal sockets
                                                       (partial internal-handler poller))
                              (catch Exception e
                                (log/error e "Error while handling internal socket")
                                sockets))]
                    (recur res))
                  (recur sockets)))
            (do (.close internal)
                (doseq [[_ s] sockets]
                  (.close s)))))
        (finally
          (.close internal))))))

(defn command-loop
  [{:keys [zmq commands id stopper]}]
  (let [internal (doto (.socket zmq ZMQ/PUSH)
                   (.connect (str "inproc://lucky_internal_" id))
                   (.setLinger 0))]
    (async/go-loop [connections {}]
      (if-let [[command id & tail] (async/<! commands)]
        (case command
          :request
          (do (send-all internal (concat ["REQUEST" id] (first tail)))
              (recur connections))
          :reply
          (if-let [replies (get connections id)]
            (do (async/>! replies (first tail))
                (recur connections))
            (do (log/warn "Can't find connection" id)
                (recur connections)))
          :error
          (if-let [replies (get connections id)]
            (let [[error] tail]
              (log/error error "Error in connection")
              (async/close! replies)
              (recur (dissoc connections id)))
            (do (log/warn "Can't find connection" id)
                (recur connections)))
          :register
          (let [[endpoints replies] tail]
            (send-all internal (concat ["CONNECT" id] endpoints))
            (recur (assoc connections id replies)))
          :deregister
          (do (send-all internal ["DISCONNECT" id])
              (recur (dissoc connections id))))
        (do
          (.close internal)
          (async/>! stopper true))))))

(def COMMANDS-BUFFER 100)
(def IO->COMMANDS-BUFFER 100)

(defn create
  [zmq]
  (let [stopper (async/promise-chan)
        id (utils/uuid)
        commands-ch (async/chan COMMANDS-BUFFER)
        io->commands-ch (async/chan IO->COMMANDS-BUFFER)
        internal (doto (.socket zmq ZMQ/PULL)
                   (.setLinger 0)
                   (.bind (str "inproc://lucky_internal_" id)))]
    (command-loop {:zmq zmq
                   :id id
                   :commands commands-ch
                   :stopper stopper})
    (io-loop {:zmq zmq
              :id id
              :stopper stopper
              :commands commands-ch
              :internal internal})
    commands-ch))

(def REQUESTS-BUFFER 100)
(def REPLIES-BUFFER 100)

(defn register
  [reactor endpoints]
  (let [requests (async/chan REQUESTS-BUFFER)
        replies (async/chan REPLIES-BUFFER)
        id (utils/uuid)]
    (async/go
      (async/>! reactor [:register id endpoints replies])
      (loop []
        (if-let [v (async/<! requests)]
          (do (async/>! reactor [:request id v])
              (recur))
          (async/>! reactor [:deregister id]))))
    [requests replies]))

(defrecord Reactor [zmq]
  component/Lifecycle
  (start [this] (assoc :ch (create)))
  (stop [this] (async/close! (:ch this)) this))

(defn component []
  (->Reactor []))
