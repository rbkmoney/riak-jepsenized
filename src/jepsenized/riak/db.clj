(ns jepsenized.riak.db
  (:require [clojure.string :as str]
            [clj-http.client :as httpc]
            [tea-time.core :as tt]
            [jepsenized.riak.client :as client]
            [jepsen.core :as core]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn- wait-for
  "Waits for `f?` to start return true for `dt` seconds at most."
  ([f? to]
   (wait-for f? to 5))
  ([f? to delay]
   (let [deadline (+ (tt/linear-time) to)]
     (loop []
       (if (< (tt/linear-time) deadline)
         (if-let [condition (f?)]
           condition
           (do (Thread/sleep (* delay 1000)) (recur)))
         false)))))

(defn- parse-ring-ownership
  "Parses a string into a mapping `nodename->vnodes-num`."
  [s]
  (->> s
       (re-seq #"\{'([a-zA-Z0-9_@\-.]+)',(\d+)\}")
       (map rest)
       (map vec)
       (map #(update % 1 read-string))
       (into {})))

(defn get-ring-ownership
  "Retrieves ring ownership (i.e. how many vnodes managed by
   which nodes) from the point of view of `node`."
  [node]
  (-> (client/stats node)
      (:ring_ownership "[]")
      (parse-ring-ownership)))

(defn ring-stable?
  "Ensures that the Riak ring is finally stable."
  [node n]
  (let [ownership (get-ring-ownership node)
        _         (info " *** Ownership @" node "/" n "=" ownership)
        ownership (->> ownership
                       (vals)
                       (filter pos?))]
    (and (= (count ownership) n)
         (->> ownership
              (map - (rest ownership))
              (map #(Math/abs %))
              (every? #(<= % 1))))))

(def ^:private log-dir  ["/var/log/riak"])
(def ^:private data-dir ["/var/lib/riak"])
(def ^:private conf-file "/etc/riak/riak.conf")
(def ^:private user-conf-file "/etc/riak/user.conf")


(defn- wipe-directory
  [dir]
  (c/exec :find dir :-mindepth 1 :-delete))

(defn- list-directory
  [dir]
  (->> (c/exec :find dir :-type :f)
       (str/split-lines)
       (filter not-empty)))

(defn- exec-script
  "Run a script with the help of basic shell, spitting out traces
   of execution to the stderr and failing on first failed statement."
  [& lines]
  (c/exec :sh :-c :-ex (str/join "\n" lines)))

(defn- append-line
  "Append a line to the end of some file."
  [filename-to line]
  (c/exec :echo line :>> filename-to))

(defn- append-file
  "Pour contents of one file into the end of another."
  [filename-to filename-from]
  (c/exec :cat filename-from :>> filename-to))

(defn- backup-file-name
  [filename] (str filename ".bak"))

(defn- backup-file
  "Leave a copy of some file with a special extension."
  [filename]
  (let [backup (backup-file-name filename)]
    (info "Backing up" filename "to" backup "...")
    (c/exec :cp :-a filename backup)))

(defn- restore-file
  "Restore original copy of some file."
  [filename]
  (let [backup (backup-file-name filename)]
    (info "Restoring" filename "from" backup "...")
    (c/exec :cp :-af backup filename)))

(defn- file-exists?
  "Returns true if specified regular file exists."
  [filename]
  (->> (str "test -f " (c/escape filename) " ; echo $?")
       (c/exec :sh :-c)
       (str/trim)
       (= "0")))

(defn- reset-file
  "If there's no backup, start with making one, otherwise restore from
   the backup."
  [filename]
  (if (file-exists? (backup-file-name filename))
    (restore-file filename)
    (backup-file filename)))

(defn- resolve-node
  "Resolve node hostname to an IP address in a hackish way."
  [node]
  (exec-script (str "ping -c1 " node " | ")
               "awk '/^PING/ {print $3}' | "
               "sed 's/[()]//g'"))

(defn- apply-cluster-conf
  [node cluster-name test]
  (info "Applying node-specific configuration...")
  (reset-file conf-file)
  (doseq [l [(str "nodename = riak@" node)
             (str "distributed_cookie = " cluster-name)
             (str "listener.protobuf.internal = " node ":" client/riak-pb-port)
             (str "listener.http.internal = " node ":" client/riak-http-port)]]
    (append-line conf-file l))
  (append-file conf-file user-conf-file))

(defn- start-node
  []
  (info "Starting up node...")
  (exec-script "riak start"
               "riak-admin wait-for-service riak_kv"))

(defn- stop-node
  []
  (info "Stopping node...")
  (exec-script "riak stop"))

(defn- node-online?
  []
  (->> (exec-script "riak ping || echo offline")
       (str/trim)
       (not= "offline")))

(defn- join-cluster
  [coord-node]
  (info "Joining the cluster at" coord-node "...")
  (exec-script (str "riak-admin cluster join riak@" coord-node)
               "riak-admin cluster plan"
               "riak-admin cluster commit"))

(defn- leave-cluster
  []
  (info "Leaving the cluster...")
  (exec-script "riak-admin cluster leave"
               "riak-admin cluster plan"
               "riak-admin cluster commit"))

(defn- wait-ring-stable
  [node size to]
  (info "Waiting" to "seconds for ring to become stable...")
  (when-not (wait-for #(ring-stable? node size) to)
    (warn "Ring is still unstable, there are ongoing transfers!")))

(defrecord DB [cluster-name opts]

  db/DB
  (setup! [this test node]
    (let [node (resolve-node node)
          seed (resolve-node (first (:nodes test)))
          size (count (:nodes test))]
      (apply-cluster-conf node cluster-name conf-file)
      (start-node)
      (core/synchronize test)
      (when (not= node seed) (join-cluster seed))
      (wait-ring-stable node size (:ring-stable-delay opts 10))))

  (teardown! [this test node]
    (let [node (resolve-node node)
          seed (resolve-node (first (:nodes test)))
          size (count (:nodes test))]
      (when (node-online?)
        (when (not= node seed) (leave-cluster))
        (wait-ring-stable node size (:ring-stable-delay opts 10))
        (core/synchronize test)
        (stop-node))
      (doseq [dir [log-dir data-dir]]
        (wipe-directory dir))))

  db/LogFiles
  (log-files [this test node]
    (list-directory log-dir)))

(defn update-opts
  "Redefine a subset of DB options."
  [db opts]
  (update db :opts merge opts))

(defn db
  "Riak instance."
  [opts]
  (->> (select-keys opts [:ring-stable-delay])
       (DB. "jepsen")))
