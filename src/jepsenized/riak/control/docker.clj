(ns jepsenized.riak.control.docker
  (:require [jepsen.control :as c]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [slingshot.slingshot :refer [throw+]]))

(defn- unwrap-result
  "Throws when shell returned with nonzero exit status."
  [exc-type {:keys [exit] :as result}]
  (if (zero? exit)
    result
    (throw+
     (assoc result :type exc-type)
     nil ; cause
     "Command exited with non-zero status %d:\nSTDOUT:\n%s\n\nSTDERR:\n%s"
     exit
     (:out result)
     (:err result))))

(defn- unwrap-stdout
  "Extracts stdout. Throws when shell returned with nonzero exit status."
  [result]
  (->> result
       (unwrap-result ::nonzero-exit)
       (:out)))

(defn- mk-ps-filter
  "Constructs a filter expression for `docker ps` given docker-compose service name."
  [service]
  (str "label=com.docker.compose.service=" service))

(defn resolve-container
  "Finds container-id given docker-compose service name."
  [service]
  (->> service
       (mk-ps-filter)
       (c/escape)
       (sh/sh "docker" "ps" "-q" "--filter")
       (unwrap-stdout)
       (str/trim-newline)))

(defn exec
  "Execute a shell command in a docker container under docker-compose environment."
  [container-id {:keys [cmd] :as opts}]
  (apply sh/sh
         "docker" "exec" (c/escape container-id)
         "sh" "-c" cmd
         (if-let [in (:in opts)]
           [:in in]
           [])))

(defn- path->container
  [container-id path]
  (str container-id ":" path))

(defn cp-to
  "Copies files from the host to a container filesystem."
  [container-id local-paths remote-path]
  (doseq [local-path (flatten [local-paths])]
    (->> (sh/sh
          "docker" "cp"
          (c/escape local-path)
          (c/escape (path->container container-id remote-path)))
         (unwrap-result ::copy-failed))))

(defn cp-from
  "Copies files from a container filesystem to the host."
  [container-id remote-paths local-path]
  (doseq [remote-path (flatten [remote-paths])]
    (->> (sh/sh
          "docker" "cp"
          (c/escape (path->container container-id remote-path))
          (c/escape local-path))
         (unwrap-result ::copy-failed))))

(defrecord DockerRemote [container-id]
  c/Remote
  (connect [this node]
    (assoc this :container-id (resolve-container (name node))))

  (disconnect! [this]
    (dissoc this :container-id))

  (execute! [this action]
    (exec container-id action))

  (upload! [this local-paths remote-path _]
    (cp-to container-id local-paths remote-path))

  (download! [this remote-paths local-path _]
    (cp-from container-id remote-paths local-path)))

(def remote
  "A remote that does things via docker cmdline interface."
  (DockerRemote. nil))
