(ns lucky-client.reactor
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [lucky-client.utils :as utils]
            [clojure.tools.logging :as log])
  (:import [org.zeromq ZMQ ZMQ$Poller ZMQ$Context ZMQ$Socket ZMQ$PollItem]))

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
  [^ZMQ$Socket socket values]
  (let [send-f (fn [v]
                 (cond
                   (string? v)
                   (.send socket ^String v)
                   (utils/byte-array? v)
                   (.send socket ^bytes v)
                   :else
                   (throw (Exception. (str "Can't send to zmq type " (type v))))))
        send-more-f (fn [v]
                      (cond
                        (string? v)
                        (.sendMore socket ^String v)
                        (utils/byte-array? v)
                        (.sendMore socket ^bytes v)
                        :else
                        (throw (Exception. (str "Can't send to zmq type " (type v))))))]
    (loop [[v & values] values]
      (when v
        (if (empty? values)
          (send-f v)
          (do (send-more-f v)
              (recur values)))))))

(defn recv-all
  [^ZMQ$Socket socket]
  (loop [acc ()]
    (let [data (.recv socket ZMQ/DONTWAIT)]
      (when data
        (let [acc' (cons data acc)]
          (if (.hasReceiveMore socket)
            (recur acc')
            (reverse acc')))))))

(def FORCE-STOP-TIMEOUT (* 1000000000 5))

(defn io-loop
  [{:keys [^ZMQ$Context zmq stopper id commands ^ZMQ$Socket internal]}]
  (async/thread
    (let [;; internal (doto (.socket zmq ZMQ/PULL)
          ;;            (.setLinger 0)
          ;;            (.bind (str "inproc://lucky_internal_" id)))
          poller (doto ^ZMQ$Poller (.poller zmq)
                   (.setTimeout 500)
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
          (fn [^ZMQ$Poller poller sockets [^bytes command ^bytes socket-id & tail]]
            (let [socket-id' (String. socket-id)]
              (case (String. command)
                "CONNECT"
                (try
                  (let [[type & endpoints] tail
                        ^ZMQ$Socket socket (doto (.socket zmq (case (String. type)
                                                                "DEALER" ZMQ/DEALER
                                                                "ROUTER" ZMQ/ROUTER))
                                             (.setLinger 1000))]
                    (doseq [^bytes endpoint endpoints]
                      (.connect socket (String. endpoint)))
                    (.register poller socket ZMQ$Poller/POLLIN)
                    (cons [socket-id' socket] sockets))
                  (catch Exception e
                    (log/error e "Can't connect")
                    (async/>!! commands [:error socket-id' e])
                    sockets))
                "DISCONNECT"
                (if-let [^ZMQ$Socket socket (utils/alist-find-by-k sockets socket-id')]
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
          (fn [sockets ^ZMQ$PollItem item]
            (let [socket (.getSocket item)]
              (if-let [id (utils/alist-find-by-v sockets socket)]
                (handle-socket-f socket (partial external-handler id))
                (log/error "Unknown socket in IO loop (from external)"))))]
      (loop [force-stop-at nil sockets ()]
        (cond
          (and (async/poll! stopper) (nil? force-stop-at))
          (recur (+ (utils/now) FORCE-STOP-TIMEOUT) sockets)

          (and force-stop-at (zero? (count sockets)))
          (do (.close internal)
              nil)

          (and force-stop-at (< force-stop-at (utils/now)))
          (do (log/warn "Force stopping reactor")
              (.close internal)
              (doseq [[_ ^ZMQ$Socket s] sockets]
                (.close s)))

          :else
          (do
            (.poll poller)
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
                (recur force-stop-at res))
              (recur force-stop-at sockets))))))))

(defn command-loop
  [{:keys [^ZMQ$Context zmq commands id stopper]}]
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
          (let [[type endpoints replies] tail]
            (send-all internal (concat ["CONNECT" id (str type)] endpoints))
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
  [^ZMQ$Context zmq]
  (let [stopper (async/promise-chan)
        id (utils/uuid)
        commands-ch (async/chan COMMANDS-BUFFER)
        io->commands-ch (async/chan IO->COMMANDS-BUFFER)
        internal (doto (.socket zmq ZMQ/PULL)
                   (.setLinger 0)
                   (.bind (str "inproc://lucky_internal_" id)))
        _ (command-loop {:zmq zmq
                         :id id
                         :commands commands-ch
                         :stopper stopper})
        process (io-loop {:zmq zmq
                          :id id
                          :stopper stopper
                          :commands commands-ch
                          :internal internal})]
    [(fn [] (async/close! commands-ch) (async/<!! process))
     commands-ch]))

(def REQUESTS-BUFFER 100)
(def REPLIES-BUFFER 100)

(defn register
  [reactor type endpoints]
  (let [requests (async/chan REQUESTS-BUFFER)
        replies (async/chan REPLIES-BUFFER)
        id (utils/uuid)]
    (async/go
      (async/>! reactor [:register id (name type) endpoints replies])
      (loop []
        (if-let [v (async/<! requests)]
          (do (async/>! reactor [:request id v])
              (recur))
          (async/>! reactor [:deregister id]))))
    [requests replies]))

(defrecord Reactor [zmq]
  component/Lifecycle
  (start [this]
    (let [[stopper commands] (create (:context zmq))]
      (assoc this
             :commands commands
             :stopper stopper)))
  (stop [this]
    ((:stopper this))
    this))

(defn component []
  (map->Reactor {}))
