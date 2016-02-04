(ns clj-jedis.core
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

(defn hscan
  ([k cursor]
     (let [result (.hscan ^Jedis *jedis* k (str cursor))]
       {:cursor (.getCursor result)
        :result (map #(vector (.getKey %) (.getValue %))
                     (.getResult result))})))

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
  (map georadiusresponse  (.georadius ^Jedis *jedis* k lon lat (double radius) (geo-unit unit))))

(defn get [k]
  (.get ^Jedis *jedis* k))

(defn hset
  ([k f v]
      (.hset ^Jedis *jedis* k f v)))

(defn hsetnx [k f v]
  (.hsetnx ^Jedis *jedis* k f v))

(defn hget [k f]
  (.hget ^Jedis *jedis* k f))

(defn hgetall [k]
  (reduce (fn [c x]
            (println "c=" c)
            (println "x=" x)
            (assoc c (.getKey x) (.getValue x)))
          {}
          (.hgetAll ^Jedis *jedis* k)))

(defn hkeys [k]
  (.hkeys ^Jedis *jedis* k))

(defn hexists [k f]
  (.hexists ^Jedis *jedis* k f))

(defn hmset
  ([k fvm]
     (.hmset ^Jedis *jedis* k fvm))
  ([k f v & fvs]
     (println fvs)
    (.hmset ^Jedis *jedis* k (merge {f v} (apply hash-map fvs)))))

(defn hmget [k & fs]
  (.hmget ^Jedis *jedis* k (into-array fs)))

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
