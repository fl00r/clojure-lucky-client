(ns lucky-client.utils
  (:import [java.util UUID]))

(defn uuid
  []
  (-> (UUID/randomUUID)
      str))

(defn alist-find-by-k
  [alist k]
  (some (fn [[k' v]] (when (= k k') v)) alist))

(defn alist-find-by-v
  [alist v]
  (some (fn [[k v']] (when (= v v') k)) alist))

(defn alist-remove
  [alist k]
  (filter (fn [[k' v]] (not= k k')) alist))

(defn now
  []
  (System/nanoTime))
