(ns clj-jedis.core
  (:require [clojure.set])
  (:import [redis.clients.jedis
            GeoCoordinate
            GeoRadiusResponse
            GeoUnit
            HostAndPort
            Jedis
            JedisPool
            JedisCluster]))

(def ^:dynamic *jedis* nil)

(defprotocol JedisProvider
  (get-conn [this])
  (release-conn [this conn]))

(defmacro with-jedis [jedis & body]
  `(binding [*jedis* (get-conn ~jedis)]
     (try
       ~@body
       (finally
         (release-conn ~jedis *jedis*)))))

(extend-protocol JedisProvider
  JedisPool
  (get-conn [this]
    (.getResource this))
  (release-conn [this conn]
    (.close conn))

  JedisCluster
  (get-conn [this] this)
  (release-conn [this conn])

  Jedis
  (get-conn [this] this)
  (release-conn [this conn]))

(defmacro pool
  "Define a pool"
  [host-port & options]

  ;; TODO - parse options
  `(let [[host# port#] (clojure.string/split ~host-port #":")
         int-port# (Integer/parseInt port#)]
     (JedisPool. host# int-port#)))

(defmacro cluster
  "Define a cluster.

  cluster - comma-seperated host:port pairs"

  [host-port-seq]

  `(->> (clojure.string/split ~host-port-seq #",")
        (map #(clojure.string/split % #":"))
        (map #(HostAndPort. (first %1) (Integer/parseInt (second %1))))
        (apply hash-set)
        (JedisCluster.)))

;; Utility commands
(defn truthy? [value]
  (or (true? value) (#{"1" "OK"} (str value))))

(defn falsy? [expr]
  (not (truthy? expr)))

(defn- seq-convert
  ([c [k v]]  (if v (conj c k v) c))
  ([f c [k v]]
     (if v (conj c k (f k v)) c)))

(defn- map-convert
  ([c [k v]] (assoc c k v))
  ([f c [k v]] (assoc c k (f k v))))

#_(defmacro prn-> [arg ]
  `(do (println "arg-->" ~arg)
       ~arg))

(defn value-converter [f]
  (let [strfn #(if % (str %))]
    (fn [k v]
      ((or (f k) strfn) v))))



(defn hmencode
  "Convert a map 'm' into a seq of key-value pairs, replacing
each key with value in source map with value from km map.
Nil values in source map are eliminated"
  ([m km] (hmencode m km nil))
  ([m km f]

     (let [r (if f (partial seq-convert f) seq-convert)]

       (->> (clojure.set/rename-keys m km)
            (reduce r [])))))

(defn hmdecode
  "Opposite of hmencode, convert seq into map using km as
lookup table"
  ([s km] (hmdecode s km nil ))
  ([s km f] (hmdecode s km nil f))
  ([s km ns f]
     (let [r (if f (partial map-convert f) map-convert)
           m (-> (reduce r {} (partition 2 s)) (clojure.set/rename-keys km))
           d (->> m (keys) (apply hash-set) (clojure.set/difference ns))
           d (interleave d (repeat nil))
           m (->> d (partition 2) (reduce map-convert m))]

       m)))

;; Bridge commands - till they get implemented by JedisCluster
(defn keys [pattern]
  (vec (.eval ^Jedis *jedis* "return redis.call('keys',KEYS[1])" [pattern] [])))

;; Redis commands

(defn flushall []
  (.flushAll *jedis*))

(defn flushdb []
  (.flushDB  *jedis*))

(defn del [k]
  (.del ^Jedis *jedis* k))

(defn scan [cursor pattern]
  (.scan ^Jedis *jedis* cursor pattern))

(defn geo-unit [unit]
  "Converts :km :mi :m :ft into GeoCoordinate"
  (condp = unit
    :m (GeoUnit/M)
    :mi (GeoUnit/MI)
    :ft (GeoUnit/FT)
    :km (GeoUnit/KM)))

(defn geoadd
  ([k lon lat m]
     (.geoadd ^Jedis *jedis* k lon lat m))
  ([k ms]
     (.geoadd ^Jedis *jedis* k ms)))

(defn geodist
  ([k m1 m2]
     (.geodist ^Jedis *jedis* k m1 m2))
  ([k m1 m2 unit]
     (.geodist ^Jedis *jedis* k m1 m2 (geo-unit unit))))

(defn geocord [lon lat]
  (GeoCoordinate. lon lat))

(defn- georadiusresponse [^GeoRadiusResponse r]
  (.getMemberByString r))

(defn georadius [k lon lat radius unit]
  (map georadiusresponse
       (.georadius ^Jedis *jedis* k
                   (double lon) (double lat) (double radius) (geo-unit unit))))

(defn- geoposresponse [^GeoCoordinate gc]
  {:lat (str (.getLatitude gc)) :lon (str (.getLongitude gc))})

(defn geopos [k & v]
  (map geoposresponse (.geopos ^Jedis *jedis* k (into-array v))))

(defn get [k]
  (.get ^Jedis *jedis* k))

(defn exists [k]
  (truthy? (.exists ^Jedis *jedis* k)))

(defn hset
  ([k f v]
      (.hset ^Jedis *jedis* k f v)))

(defn expire [k secs]
  (.expire ^Jedis *jedis* k secs))

(defn lpush [k & v]
  (.lpush ^Jedis *jedis* k (into-array v)))

(defn lpushx
  ([k & v]
     (loop [v v]
       (if (first v)
         (do
           (.lpushx ^Jedis *jedis* k (into-array [(first v)]))
           (recur (rest v)))))))

(defn lrange [k from to]
  (vec (.lrange ^Jedis *jedis* k from to)))

(defn rpush [k & v]
  (.rpush ^Jedis *jedis* k (into-array v)))

(defn rpop [k]
  (.rpop ^Jedis *jedis* k))

(defn rpoplpush [src dest]
  (.rpoplpush ^Jedis *jedis* src dest))

(defn hsetnx [k f v]
  (assert (= java.lang.String (type v)))
    (assert (= java.lang.String (type f)))
  (truthy? (.hsetnx ^Jedis *jedis* k f v)))

(defn hdel [h & fields]
  (.hdel ^Jedis *jedis* h (into-array fields)))

(defn hget [k f]
  (.hget ^Jedis *jedis* k f))

(defn hgetall [k]
  (reduce (fn [c x]
            (assoc c (.getKey x) (.getValue x)))
          {}
          (.hgetAll ^Jedis *jedis* k)))

(defn hkeys [k]
  (.hkeys ^Jedis *jedis* k))

(defn hexists [k f]
  (truthy? (.hexists ^Jedis *jedis* k f)))

(defn hmset
  ([k fvm]
     (.hmset ^Jedis *jedis* k fvm))
  ([k f v & fvs]
    (.hmset ^Jedis *jedis* k (merge {f v} (apply hash-map fvs)))))

(defn hmget [k & fs]
  (.hmget ^Jedis *jedis* k (into-array fs)))

(defn hscan
  ([k] (hscan k nil))
  ([k c]
     (let [scan-result (.hscan ^Jedis *jedis* k (or c "0"))
           cursor (.getStringCursor scan-result)
           result (.getResult scan-result)
           result-map (reduce (fn [c v]
                                (assoc c (.getKey v) (.getValue v))) {} result)]
       {:cursor (if (= "0" cursor) nil cursor) :result result-map})))

(defn incr [k]
  (.incr ^Jedis *jedis* k))

(defn decr [k]
  (.decr ^Jedis *jedis* k))

(defn mget [& ks]
  (.mget ^Jedis *jedis* (into-array ks)))

(defn mset [& ks]
  (.mset ^Jedis *jedis* (into-array ks)))

(defn set [k v]
  (.set ^Jedis *jedis* k v))


(defn sadd [k & vs]
  (.sadd ^Jedis *jedis* k (into-array vs)))

(defn smembers [k]
  (.smembers ^Jedis *jedis* k))

(defn srem [k & vs]
  (.srem ^Jedis *jedis* k (into-array vs)))

(defn sscan
  ([k] (sscan k nil))
  ([k c]

     (let [tuple (.sscan ^Jedis *jedis* k (or c "0"))
           cursor (.getStringCursor tuple)
           result (.getResult tuple)]
       {:cursor (if (= "0" cursor) nil cursor) :result result})))

(defn zrem [k & v]
  (.zrem ^Jedis *jedis* k (into-array v)))

(defn zscan
  ([k] (zscan k nil))
  ([k c]

     (let [tuple (.zscan ^Jedis *jedis* k (or c "0"))
           cursor (.getStringCursor tuple)
           result (.getResult tuple)
           result (map #(vector (.getElement %) (.getScore %)) result)]
       {:cursor (if (= "0" cursor) nil cursor) :result result})))
