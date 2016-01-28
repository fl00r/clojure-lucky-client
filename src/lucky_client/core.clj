(ns lucky-client.core)

(defrecord Request [id body timeout])

(defrecord Reply [id body])

(defrecord Connect [id endpoints])

(defrecord Disconnect [id])

(defrecord Connection [])

(defn reactor
  [])

(defn register
  [])

(defn deregister
  [])

(defn request
  [])


