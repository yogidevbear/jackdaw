(ns word-count-test
  (:require
   [word-count :as wc]
   [jackdaw.streams :as k]
   [jackdaw.streams.protocols :refer [streams-builder*]]
   [jackdaw.streams.mock :as mock]
   [jackdaw.test :as jd.test]
   [jackdaw.test.fixtures :as fix]
   [clojure.test :as t :refer [deftest is testing]])
  (:import
   (java.util Properties)
   (org.apache.kafka.streams TopologyTestDriver)))

(def broker-config
  {"bootstrap.servers" "localhost:9092"})

(def app-config
  (assoc broker-config
         "cache.max.bytes.buffering" "0"
         "application.id" "streams-word-count"
         "default.key.serde" "org.apache.kafka.common.serialization.Serdes$StringSerde"
         "default.value.serde" "org.apache.kafka.common.serialization.Serdes$StringSerde"))

(def test-consumer-config
  (assoc broker-config
         "group.id" "word-count-test"))

(defn input-writer
  "Helper for generating input commands. For each line, we return a `:write!`
   command that will produces a record when executed by the test-machine
   (in this case with k = v = line)"
  [line]
  [:write! :input line {:key-fn identity}])

(defn word-watcher
  "Builds a test-command that blocks until the supplied word appears"
  [word]
  [:watch (fn [journal]
            (some #(= word (:key %))
                  (get-in journal [:topics :output]))) 2000])

(defn wc
  "A simple helper to extract the latest value from the word-count ktable
   as observed by the test-consumer.

   The journal collects all records as a vector of maps representing
   ConsumerRecords for each topic. Since we're inspecting a mutating
   table, we want to get the `last` matching record for `word`."
  [journal word]
  (->> (get-in journal [:topics :output])
       (filter (fn [r]
                 (= (:key r) word)))
       last
       :value))

(defn props-for [x]
  (doto (Properties.)
    (.putAll (reduce-kv (fn [m k v]
                          (assoc m (str k) (str v)))
                        {}
                        x))))

(defn mock-transport-config
  []
  {:driver (let [builder (k/streams-builder)
                 app (wc/word-count wc/word-count-topics)
                 topology (.build (streams-builder* (app builder)))]
             (TopologyTestDriver.
              topology
              (props-for wc/app-config)))})

(deftest test-word-count-demo
  (fix/with-fixtures [(fix/empty-state-fixture wc/app-config)]
    (fix/with-test-machine (jd.test/mock-transport (mock-transport-config) wc/word-count-topics)
      (fn [machine]
        (let [lines ["As Gregor Samsa awoke one morning from uneasy dreams"
                     "he found himself transformed in his bed into an enormous insect"
                     "What a fate: to be condemned to work for a firm where the"
                     "slightest negligence at once gave rise to the gravest suspicion"
                     "How about if I sleep a little bit longer and forget all this nonsense"
                     "I cannot make you understand"]
              commands (->> (concat
                             (map input-writer lines)
                             [(word-watcher "understand")]))

              {:keys [results journal]} (jd.test/run-test machine commands)]

          (is (every? #(= :ok (:status %)) results))

          (is (= 1 (wc journal "understand")))
          (is (= 2 (wc journal "i")))
          (is (= 3 (wc journal "to"))))))))
