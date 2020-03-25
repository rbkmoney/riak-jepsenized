(ns jepsenized.riak.main
  (:require [clj-time.core :as time]
            [clj-http.client :as httpc]
            [cheshire.core :refer [parse-string]]
            [tea-time.core :as tt]
            [clojure.string :as str]
            [jepsenized.riak.control.docker :as docker]
            [jepsen.db :as db]
            [jepsenized.riak.db :as riak]
            [jepsen.cli :as cli]
            [jepsen.os :as os]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.checker :as checker]
            [jepsen.independent :as indep]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsenized.riak.cli-utils :as cli-utils]
            [jepsenized.riak.register-test :as register-test])
  (:import (java.util.concurrent CyclicBarrier)))

; nemeses

(defn cluster-scaler
  "Join extra nodes to the cluster, effectively running db startup routine
   on each of them when ordered to start, and tearing down each of them
   gracefully when ordered to stop."
  [extra-nodes stop-db-opts]
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (let [f    (:f op)
            riak (:db test)
            size (count extra-nodes)
            ; tweak cyclic barrier with number of _extra_ nodes, instead of total
            test (assoc test :barrier (if (pos? size)
                                        (CyclicBarrier. size)
                                        ::no-barrier))]
        (c/with-remote (:remote test)
          (case f
            :start
            (let [test (update test :nodes #(concat % extra-nodes))] ; append extra nodes
              (c/on-many extra-nodes (db/setup! riak test c/*host*)))
      
            :stop
            (let [riak (riak/update-opts riak stop-db-opts)] ; override db opts on stop only
              (c/on-many extra-nodes (db/teardown! riak test c/*host*))))

          (assoc op
                 :type :info
                 :value extra-nodes))))

    (teardown! [this test]
      this)))

(def nemeses
  {"nothing"        (constantly nemesis/noop)
   "cluster-scaler" (partial cluster-scaler ["extra1" "extra2"])})

; workloads

(defn mk-unique-object-id
  "Make unique object id to allow reuse running SUT."
  [prefix]
  (let [unique-part (->> (time/now)
                         (time/interval (time/date-time 2000 01 01))
                         (time/in-seconds))]
    (str prefix unique-part)))

(def workloads
  {"register" register-test/workload})

; cli setup

(def cli-opts
  "Additional command line options."
   ; workload-related options
  [(concat
    [nil "--keyspace SIZE" "How big is the keyspace? Number of independent entities to test and track."
     :default [:fraction :concurrency 0.25] ; four threads per key by default
     :default-desc "0.25c"]
    (cli-utils/number-or-fraction :concurrency))

   [nil "--workload WORKLOAD" "Which workload to test exactly?"
    :missing (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]

    ; nemesis-related options
   [nil "--simulate NEMESIS" "Which havoc to wreak exactly?"
    :missing (str "--simulate " (cli/one-of nemeses))
    :validate [nemeses (cli/one-of nemeses)]]

   (cli-utils/pos-int-opt "--havoc-delay SECS" "How long to wait before wreaking havoc?" 30)
   (cli-utils/pos-int-opt "--havoc-duration SECS" "How long to wreak havoc?" 30)

   (cli-utils/pos-int-opt "--node-leave-timeout SECS"
                          "How long to wait for node to leave cluster cleanly?"
                          120)

    ; the rest
   [nil "--rate HZ" "Approximate number of request per second, per thread."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number"]]])

(defn- gen-concurrent
  "Instantiate a concurrent generator given `opts`."
  [opts gen]
  (let [object-id  (mk-unique-object-id "jepsen")
        object-ids (map #(str object-id "-" %) (range))
        {:keys [keyspace concurrency]} opts]
    (indep/concurrent-generator (quot concurrency keyspace)
                                object-ids
                                (fn [_] gen))))

(defn mk-test
  "Make a test map given options."
  [opts]
  (let [opts       (cli-utils/resolve-opts opts)
        wl-name    (:workload opts)
        workload   (get workloads wl-name)
        work       (workload opts)
        simulation (:simulate opts)
        nemesis    (get nemeses simulation)
        time-limit (:time-limit opts 300)
        op-delay   (/ (:rate opts))]
    (merge tests/noop-test
           opts
           {:name      (str (:name work) "-" simulation)
            :os        os/noop
            :remote    docker/remote
            :db        (riak/db {})
            :client    (:client work)
            :nemesis   (nemesis opts)
            :generator (gen/phases
                        (->> (gen-concurrent
                              opts
                              (gen/stagger op-delay (:generator work)))
                             (gen/nemesis (gen/start-stop (:havoc-delay opts) (:havoc-duration opts)))
                             (gen/time-limit time-limit))
                        (gen/nemesis (gen/once {:type :info :f :stop})))
            :checker   (checker/compose
                        {:workload (indep/checker (:checker work))
                         :stats    (checker/stats)
                         :perf     (checker/perf)})})))

(defn -main
  "Handles command lines arguments."
  [& args]
  (cli/run! (cli/single-test-cmd
             {:test-fn         mk-test
              :opt-spec        cli-opts})
            args))
