(defproject riak-jepsenized "0.1.0-SNAPSHOT"
  :description "Obliterate Riak KV with Jepsen framework"
  :url "http://github.com/rbkmoney/riak-jepsenized"
  :license {:name "Apache-2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.match "0.3.0"]
                 [javax.xml.bind/jaxb-api "2.3.1"] ; for compatibility w/ JDK ≥ 11
                 [org.blancas/kern "1.1.0"]
                 [jepsen "0.1.17"]
                 [clj-http "3.10.0"]
                 [com.basho.riak/riak-client "2.1.1"]
                 [com.yetanalytics/kria "0.2.0"]
                 [cheshire "5.8.1"]]
  :main jepsenized.riak.main
  :jvm-opts ["-Xmx6g"])
