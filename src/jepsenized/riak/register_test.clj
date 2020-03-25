(ns jepsenized.riak.register-test
  (:require [jepsenized.riak.client :as riak]
            [kria.client :as rc]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as indep]
            [knossos.model :as model]))

(defrecord Client [seen c bucket nval]

  client/Client
  (open! [this _ node]
    (assoc this :c (riak/mk-client node)))

  (setup! [this test]
    (riak/set-quorum-bucket c bucket nval))

  (invoke! [this test op]
    (riak/with-client c op
      (let [[k v] (:value op)]
        (case (:f op)
          :read (let [obj (riak/get-object bucket k)
                      v   (riak/value obj)]
                  (assoc op
                         :type :ok
                         :value (indep/tuple k v)
                         :vclock (riak/vclock obj)))
          :write (loop []
                   (if-let [objctx (locking seen (get @seen k))]
                     ; races are possible here, but we do not care much about them
                     ; because they're extremely unlikely
                     (locking objctx
                       (let [old @objctx
                             new (riak/put-object bucket k v old)]
                         (assert (riak/found? new))
                         (reset! objctx new)
                         (assoc op
                                :type :ok
                                :vclock (mapv riak/vclock [old new]))))
                     ; else
                     (do
                       (swap! seen assoc! k (atom {}))
                       (recur))))
          :cas (assoc op
                      :type :not-implemented)))))

  (teardown! [this test]
    ())

  (close! [this test]
    (rc/disconnect c)))

(defn client
  "Instantiates client prototype."
  [bucket nval]
  (Client. (atom (transient {})) nil bucket nval))

(defn gen-read-write
  "Random write/read ops for a register over a small field of integers."
  [space-size]
  (reify gen/Generator
    (op [generator test process]
      (condp < (rand)
        0.5 {:type  :invoke
             :f     :read}
        {:type  :invoke
         :f     :write
         :value (rand-int space-size)}))))

(defn workload
  "Instantiate linearizable register test workload."
  [opts]
  (let [bucket (:bucket opts "registers")
        nval   (:nval   opts 3)]
    {:name      "riak-register"
     :client    (client bucket nval)
     :checker   (checker/linearizable {:model (model/cas-register)})
     :generator (gen-read-write 5)}))
