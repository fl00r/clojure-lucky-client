(ns lucky-client.zmq
  (:import [org.zeromq ZMQ])
  (:require [com.stuartsierra.component :as component]))

(defn create []
  (doto (ZMQ/context 1)
    (.setBlocky false)))

(defrecord ZMQComponent [context]
  component/Lifecycle
  (start [this] (assoc this :context (create)))
  (stop [this] (.term (:context this)) this))

(defn component [] (map->ZMQComponent {}))
