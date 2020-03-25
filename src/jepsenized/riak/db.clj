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

(defn wait-for
  "Waits for `f?` to start return true for `dt` seconds at most."
  ([f? to]
   (wait-for f? to (-> (quot to 10) (max 2) (min 10))))
  ([f? to delay]
   (let [deadline (+ (tt/linear-time) to)]
     (loop []
       (if (< (tt/linear-time) deadline)
         (if-let [condition (f?)]
           condition
           (do (Thread/sleep (* delay 1000)) (recur))))))))

(defn wait-until
  "Like `wait-for` but waits for falsy return."
  [f? & args]
  (apply wait-for (comp not f?) args))

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
        ;; _         (info " *** Ownership @" node "/" n "=" ownership)
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

(defn- exec-command
  [line & opts]
  (let [trace? (some #{:trace} opts)
        echo?  (some #{:echo} opts)
        flags  (when trace? [:-x])]
    (when echo? (info ">>>" line))
    (let [result (apply c/exec :sh :-c (conj flags line))]
      (when echo? (doseq [r (str/split-lines result)] (info "<<<" r)))
      result)))

(defn- exec-script
  "Run a script with the help of basic shell, spitting out traces
   of execution to the stderr and failing on first failed statement."
  [& lines-opts]
  (let [[lines opts] (split-with string? lines-opts)]
    (mapv #(apply exec-command % opts) lines)))

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
  (->> (str "ping -c1 " node " | "
            "awk '/^PING/ {print $3}' | "
            "sed 's/[()]//g'")
       (exec-command)
       (str/trim)))

(defn- apply-cluster-conf
  [node cluster-name]
  (info "Applying node-specific configuration...")
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
  (->> (exec-command "riak ping || exit 0")
       (str/trim)
       (= "pong")))

(defn- ring-ready?
  []
  (->> (exec-command "riak-admin ring-status || exit 0")
       (str/split-lines)
       (some #(str/starts-with? % "Ring Ready: true"))))

(defn- wait-ring-ready
  [opts]
  (wait-for ring-ready? (:ring-ready-timeout opts)))

(defn- ensure-cluster-commit
  [opts]
  (wait-for (fn []
              (->> (exec-script "riak-admin cluster plan"
                                "riak-admin cluster commit"
                                :echo)
                   (last)
                   (str/split-lines)
                   (some #(str/starts-with? % "Cluster changes committed"))))
            (:ring-ready-timeout opts)))

(defn- join-cluster
  [coord-node opts]
  (info "Joining the cluster at" coord-node "...")
  (wait-ring-ready opts)
  (exec-command (str "riak-admin cluster join riak@" coord-node))
  (ensure-cluster-commit opts))

(defn- leave-cluster
  [opts]
  (info "Leaving the cluster...")
  (wait-ring-ready opts)
  (exec-command "riak-admin cluster leave")
  (ensure-cluster-commit opts))

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
      (reset-file conf-file)
      (apply-cluster-conf node cluster-name)
      (start-node)
      (core/synchronize test)
      (when (not= node seed)
        (locking this (join-cluster seed opts)))
      (core/synchronize test)))

  (teardown! [this test node]
    (let [node (resolve-node node)
          seed (resolve-node (first (:nodes test)))
          size (count (:nodes test))]
      (when (node-online?)
        (if-let [leave-timeout (:node-leave-timeout opts)]
          (do (when (not= node seed)
                (locking this (leave-cluster opts)))
              (when-not (wait-until node-online? leave-timeout)
                (warn "Cannot wait any longer for" node "to leave cluster, shutting down...")
                (stop-node)))
          (stop-node)))
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
  (->> (select-keys opts [:node-leave-timeout
                          :ring-ready-timeout])
       (merge {:ring-ready-timeout 15})
       (DB. "jepsen")))
