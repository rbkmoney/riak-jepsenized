(ns jepsenized.riak.client
  (:require [clj-http.client :as httpc]
            [clojure.data.codec.base64 :as base64]
            [kria.client :as riakc]
            [kria.bucket :as bucket-api]
            [kria.object :as object-api]
            [kria.conversions :refer [byte-string<-utf8-string
                                      utf8-string<-byte-string
                                      byte-array<-byte-string]]
            [slingshot.slingshot :refer [try+ throw+]]
            [cheshire.core :refer [parse-string generate-string]]))

(def riak-http-port 8098)
(def riak-pb-port   8087)

(def bucket-rw-quorum (- 0xffffffff 2))
(def bucket-rw-all    (- 0xffffffff 3))

(def ^:dynamic *client* "An active client to issue requests with." nil)

(defn- api-endpoint
  "Contructs Registers API endpoint."
  [node]
  (str "http://" node ":" riak-http-port "/"))

(defn stats
  "Retrieves a list of runtime metrics as exposed by `/stats` API."
  [node]
  (let [url (str (api-endpoint node) "/stats")
        req {:url url :method :get}]
    (try+ (-> (httpc/request req)
              (:body)
              (parse-string true))
          (catch Object _
            {}))))

(defn- then!
  "Wraps promise in a functor which delivers said promise upon
   invocation."
  [p]
  (fn [asc e a] (deliver p [e a])))

(defn- unwrap!
  "Waits for promise, then throws an exception if one was delivered,
   otherwise just returns a delivered result."
  ([p res]
   (let [[e _] @p]
     (if e (throw+ e) res)))
  ([p]
   (let [[e a] @p]
     (if e (throw+ e) a))))

(defmacro sync->>
  "Issue a `form` request, passing it synthesized callback as the
   last argument and waiting for completion, rethrowing occured
   exceptions, then passing result through `forms` in same manner
   as `->>`."
  [form & forms]
  `(let [p# (promise)]
     (do (->> p# (then!) ~form)
         (->> (unwrap! p#) ~@forms))))

(defn mk-client
  "Instantiate a client for Riak Protobuf API."
  [node]
  (let [p (promise)
        c (riakc/connect node riak-pb-port (then! p))]
    (unwrap! p c)))

(defn set-quorum-bucket
  "Set bucket options such that reads and writes performed with
   primary quorum of vnodes, essentially giving 'almost' strong
   consistency guarantees."
  ([c bucket nval]
   (let [bucket (byte-string<-utf8-string bucket)
         props {:n-val           nval
                :allow-mult      false
                :last-write-wins false
                :basic-quorum    false
                :not-found-ok    true
                :r               bucket-rw-quorum
                :pr              bucket-rw-quorum
                :w               bucket-rw-quorum
                :pw              bucket-rw-quorum
                :dw              bucket-rw-quorum}]
     (sync->> (bucket-api/set c bucket {:props props})))))

(defn get-object
  ([bucket key]
   (let [bucket (byte-string<-utf8-string bucket)
         key    (byte-string<-utf8-string key)]
     (sync->> (object-api/get *client* bucket key {})))))

(defn put-object
  ([bucket key value] (put-object bucket key value nil))
  ([bucket key value o]
   (let [bucket (byte-string<-utf8-string bucket)
         key    (byte-string<-utf8-string key)
         value  (-> value
                    (generate-string)
                    (byte-string<-utf8-string))
         value  {:value value}
         opts   {:return-body true}
         opts   (if o
                  (assoc opts :vclock (:vclock o))
                  opts)]
     (sync->> (object-api/put *client* bucket key value opts)))))

(defn delete-object
  ([bucket key] (delete-object bucket key nil))
  ([bucket key o]
   (let [bucket (byte-string<-utf8-string bucket)
         key    (byte-string<-utf8-string key)
         opts   (if o
                  {:vclock (:vclock o)}
                  {})]
     (sync->> (object-api/delete *client* bucket key opts)))))

(defn found?
  "Returns false if object `o` has no value associated with it, which
   means it was deleted or was never written, otherwise true."
  [o]
  {:pre [(map? o)]}
  (not (empty? (:content o))))

(defn ambiguous?
  "Return ture if object `o` have siblings."
  [o]
  {:pre [(map? o)]}
  (> 1 (count (:content o))))

(defn value
  "Retrieve a value of object `o`, or `nil` for no value."
  [o]
  {:pre [(map? o)]}
  (if-let [content (first (:content o))]
    (-> (:value content)
        (utf8-string<-byte-string)
        (parse-string true))))

(defn vclock
  "Retrieve a vclock of object `o` in a printable representation,
   or `nil` if `o` is empty."
  [o]
  {:pre [(map? o)]}
  (if-let [vclock (:vclock o)]
    (-> vclock
        (byte-array<-byte-string)
        (base64/encode)
        (String. "UTF-8"))))

(defn map-error-message
  [msg]
  (let [re #"\{([a-z]+)_val_unsatisfied,(\d+),(\d+)\}"]
    (if-let [[_ val need have] (re-find re msg)]
      {:type :fail
       :error [:unsatisfied
               (keyword val)
               (read-string need)
               (read-string have)]})))

(defmacro with-client
  "A helper macro to wrap client requests with error handling."
  [client op & body]
  `(try+
    (binding [*client* ~client] ~@body)
    (catch com.google.protobuf.InvalidProtocolBufferException ex#
      (assoc ~op
             :type :unexpected
             :error ex#))
    (catch java.net.SocketTimeoutException ex#
      (assoc ~op
             :type (if (= :read (:f ~op)) :fail :info)
             :error :client-timeout))
    (catch map? ex#
      (if-let [error# (map-error-message (:message ex#))]
        (merge ~op error#)
        (throw+)))))

(defmacro with-node
  "A helper macro to issue requests with a one-off client."
  [node op & body]
  `(try
     (with-open [c# (mk-client ~node)]
       (with-client c# ~op ~@body))
     (catch java.net.ConnectException ex#
       (assoc ~op
              :type :fail
              :error :connect-error))))
