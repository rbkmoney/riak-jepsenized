(ns jepsenized.riak.cli-utils
  (:require [blancas.kern.core :as k]
            [clojure.core.match :refer [match]]))

(defn pos-int-opt
  "Shortcut for a CLI option for positive integer value."
  ([form desc]
   [nil form desc
    :parse-fn read-string
    :validate [pos-int? "Must be a positive integer"]])
  ([form desc default]
   (conj (pos-int-opt form desc)
         :default default)))

(defn- fraction-unit
  [of]
  (first (name of)))

(defn number-or-fraction-parser
  "Parse strings such as '30' or '0.333t'."
  [of str]
  (let [p-number (->> k/dec-num
                      k/<:>)
        p-fraction (->> (k/<< k/float-num (k/sym* (fraction-unit of)))
                        (k/<$> (partial conj [:fraction of]))
                        k/<:>)
        parser (k/<< (k/<|> p-fraction p-number) k/eof)
        parse (k/parse parser str)]
    (if (:error parse)
      [:error (k/print-error parse)]
      (:value parse))))

(defn number-or-fraction?
  "Validates if `value` is proper positive number or fraction."
  [value]
  (match [value]
    [[:fraction _ share]] (pos? share)
    :else                 (pos-int? value)))

(defn number-or-fraction
  "Make a cli option snippet for values expressed as fixed number
   or fraction of something."
  [of]
  [:parse-fn (partial number-or-fraction-parser of)
   :validate [number-or-fraction?
              (str "Must be positive number (42) or fraction (0.42" (fraction-unit of) ")")]])

(defn resolve-value
  "Resolve provided values down to scalar."
  [value opts]
  (match [value]
    [[:fraction of share]] (int (* share (get opts of)))
    :else                  value))

(defn map-values
  "Map `f` over hashmap values."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn resolve-opts
  "Resolve any unresolved values in opts."
  [opts]
  (map-values #(resolve-value % opts) opts))
