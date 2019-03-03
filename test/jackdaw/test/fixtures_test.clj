(ns jackdaw.test.fixtures-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [jackdaw.test.fixtures :refer :all])
  (:import
   (org.apache.kafka.clients.admin AdminClient NewTopic)))

(def topic-foo
  {:topic-name "foo"
   :partition-count 1
   :replication-factor 1
   :config {}})

(def kafka-config
  {"bootstrap.servers" "localhost:9092"})

(defn- topic-exists?
  [client t]
  (contains? (-> (list-topics client)
                 (.names)
                 (deref)
                 (set))
             (:topic-name t)))

(defn test-prefix
  []
  (-> (str (java.util.UUID/randomUUID))
      (.substring 0 8)))

(defn with-topic-fixture
  [{:keys [topic-config]} f]
  (let [prefix (test-prefix)
        topic-config (update topic-config
                             :topic-name #(str prefix "-" %))
        topic-fix (topic-fixture kafka-config {prefix topic-config}
                                 {:timeout-ms 1000
                                  :delete-first? true})]
    (with-fixtures [topic-fix]
      (f topic-config))))

(deftest test-topic-fixture
  (with-topic-fixture {:topic-config topic-foo}
    (fn [topic]
      (with-open [client (AdminClient/create kafka-config)]
        (is (topic-exists? client topic))))))

(defmacro with-temp-dirs [dirs & body]
  `(do
     (doseq [d# ~dirs]
       (io/make-parents (io/file (str d# "/xxx"))))
     ~@body))

(deftest test-empty-state-dir
  (with-temp-dirs ["/tmp/kafka-streams/foo"]
    (testing "delete old state"
      (with-fixtures [(empty-state-fixture {"application.id" "foo"})]
        (is (not (.exists (io/file "/tmp/kafka-streams/foo"))))))

    (testing "first run (no old state)"
      (with-fixtures [(empty-state-fixture {"application.id" "xxx"})]
        (is true))))

  (with-temp-dirs ["/tmp/kafka-streams.alt/foo"]
    (testing "updated state dir"
      (with-fixtures [(empty-state-fixture {"application.id" "foo"
                                            "state.dir" "/tmp/kafka-streams.alt"})]
        (is (not (.exists (io/file "/tmp/kafka-streams.alt/foo"))))))))

