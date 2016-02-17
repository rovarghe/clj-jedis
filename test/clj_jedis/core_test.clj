(ns clj-jedis.core-test
  (:require [clojure.test :refer :all]
            [clj-jedis.core :as jc])
  (:import [redis.clients.jedis
            JedisCluster
            JedisPool]))

(deftest test-cluster
  (is JedisCluster (type (jc/cluster "localhost:9001"))))

(deftest test-pool

  (is JedisPool (type (jc/pool "localhost:9001"))))

(def A-Z (map #(str (char (int %))) (range 65 90) ))
(def POOL (jc/pool "localhost:9001"))

(def CLUSTER (jc/cluster "localhost:9001,localhost:9002"))


#_(with-jedis CLUSTER
  (clj-jedis.core/set "A" "1"))

(deftest test-with-jedis
  (jc/with-jedis  CLUSTER
    (doall  (map #(clj-jedis.core/set % %) A-Z))
    (doall  (is A-Z (map clj-jedis.core/get A-Z)))))

(deftest test-redis-functions
  (jc/with-jedis CLUSTER
    (is (jc/truthy? true))
    (is (jc/truthy? "1"))
    (jc/set "COUNTER" "0")
    (is (pos? (count (jc/keys "C*"))))
    (is "1" (clj-jedis.core/incr "COUNTER"))
    (jc/hset "K1" "F" "Hello worldஇணைப்புகள்")
    (is (clj-jedis.core/hexists "K1" "F"))
    (is "Hello worldஇணைப்புகள்" (clj-jedis.core/hget "K1" "F"))
    (is ["F"] (clj-jedis.core/hkeys "K1"))
    (jc/hmset "J1" "F" "1" "G" "World")
    (is ["1" "World"] (clj-jedis.core/hmget "J1" "F" "G"))
    (jc/geoadd "India" 12.22 21.22 "Mumbai")
    (jc/geoadd "India" 13.11 22.11 "Goa")
    (is (pos? (clj-jedis.core/geodist "India" "Mumbai" "Goa")))
    (is (pos? (clj-jedis.core/geodist "India" "Mumbai" "Goa" :km)))
    (is ["Mumbai" "Goa"] (clj-jedis.core/georadius "India" 12.22 20.0 10000 :mi))
    (jc/set "deleteme" "1")
    (is (jc/truthy? (jc/del "deleteme")))
    (is (nil? (jc/get "deleteme")))
    (jc/set "foo" "bar")
    (jc/expire "foo" 2)

    (jc/del "foo")
    (jc/del "{foo}:$")
    (jc/lpush "foo" "1" "2" "3")
    (is (= "1"  (jc/rpop "foo")))
    (is (= "2" (jc/rpoplpush "foo" "{foo}:$")))
    (is (= "3" (jc/rpoplpush "foo" "{foo}:$")))
    (is (= [ "3" "2"] (jc/lrange "{foo}:$" 0 -1)))
    (is (= [] (jc/lrange "foo-noexists" 0 -1)))

    (jc/lpushx "foo-noexist" "1" "2")
    (is (nil? (jc/rpop "foo-noexist")))))

#_(deftest pool-test
  (test-redis-functions POOL))

#_(deftest cluster-test
  (test-redis-functions CLUSTER))

(deftest pool-test)

(defn trans-enc [k v]
  (condp = k
    "z" (name v)
    v))

(defn trans-dec [k v]

  (condp = k
    "z" (keyword v)
    v))

(deftest utility-test

  (let [in {:foo 1 :bar "233" :baz :que :k 'Foo :nil-value nil}
        km {:foo "f" :bar "b" :baz "z" :does-not-exist "nil"}
        rkm (clojure.set/map-invert km)
        enc (jc/hmencode in km trans-enc)
        de (jc/hmdecode enc rkm #{:nil-value} trans-dec)]

    (is (= in de))))

(run-tests)
