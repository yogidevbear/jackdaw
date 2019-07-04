(ns jackdaw.test.transports.rest-proxy
  (:require
   [aleph.http :as http]
   [byte-streams :as bs]
   [clojure.data.json :as json]
   [clojure.tools.logging :as log]
   [clojure.stacktrace :as stacktrace]
   [jackdaw.test.journal :as j]
   [jackdaw.test.transports :as t :refer [deftransport]]
   [jackdaw.test.serde :refer :all]
   [manifold.stream :as s]
   [manifold.deferred :as d])
  (:import
   (java.util UUID Base64)))


(def ok? #{200 204})

(defn uuid
  []
  (str (UUID/randomUUID)))

(defn- base64-encode
  [encodable]
  (let [encoder (Base64/getEncoder)]
    (-> (.encode encoder encodable)
        (String.))))

(defn- base64-decode
  [decodable]
  (when decodable
    (let [decoder (Base64/getDecoder)]
      (.decode decoder decodable))))

(defn undatafy-record
  [topic-metadata m]
  (-> m
      (update :key base64-decode)
      (update :value base64-decode)))

(def content-types
  {:byte-array "application/vnd.kafka.binary.v2+json"
   :json "application/vnd.kafka.v2+json"})

(defn rest-proxy-headers
  [accept]
  {"Content-Type" "application/vnd.kafka.binary.v2+json"
   "Accept"       accept})

(defn handle-proxy-request
  [method url headers body]
  (let [req {:throw-exceptions? false
             :headers headers
             :body (when body
                     (json/write-str body))}

        content-available? (fn [response]
                             (not (= 204 (:status response))))]

    (log/debug "> " method url (select-keys req [:status :headers :body]))

    ;; This is an asynchronous pipeline we use to process responses
    ;; from the rest-proxy. It feels a bit like `->` macro but each
    ;; step automatically waits for the promise returned by the
    ;; previous step to be delivered before using it to produce a new
    ;; promise for the next step.
    ;;
    ;; We use it here to just `assoc` on the results of attempting to
    ;; parse the json response unless the request returns a 204. If
    ;; the request returns an error, we parse it (it should still be
    ;; JSON), and stick it in `:error`. `:json-body` will remain as
    ;; nil in this case.

    (d/chain (method url req)
      #(update % :body bs/to-string)
      #(do (log/debug "< " method url (select-keys % [:status :body]))
           %)
      #(assoc % :json-body (when (content-available? %)
                             (json/read-str (:body %)
                                            :key-fn (comp keyword
                                                          (fn [x]
                                                            (clojure.string/replace x "_" "-"))))))
      #(if-not (ok? (:status %))
         (assoc % :error :proxy-error)
         %))))

(defn destroy-consumer
  [{:keys [base-uri]}]
  (let [url base-uri
        headers {"Accept" (content-types :json)}
        body nil]
    (let [response @(handle-proxy-request http/delete url headers body)]
      (when (:error response)
        (throw (ex-info "Failed to destroy consumer after use" {}))))))

(defn topic-post
  [{:keys [bootstrap-uri]} msg callback]
  (let [url (format "%s/topics/%s"
                    bootstrap-uri
                    (:topic msg))
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :byte-array)}
        body {:records [(select-keys msg [:key :value :partition])]}

        response @(handle-proxy-request http/post url headers body)]

    (let [record (when-not (:error response)
                   (-> (get-in response [:json-body :offsets])
                       first
                       (assoc :topic (:topic msg))))]
      (callback record (when (:error response)
                         response)))))

(defrecord RestProxyClient [bootstrap-uri
                            base-uri
                            group-id
                            instance-id
                            subscription])

(defn proxy-client-info [client]
  (let [object-id (-> (.hashCode client)
                      (Integer/toHexString))]
    (-> (select-keys client [:bootstrap-uri])
        (assoc :id object-id))))

(defn rest-proxy-client
  [config]
  (map->RestProxyClient config))

(defn with-consumer
  [{:keys [bootstrap-uri group-id] :as client}]
  (let [id (uuid)
        url (format "%s/consumers/%s"
                    bootstrap-uri
                    group-id)
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :json)}
        body {:name id
              :auto.offset.reset "latest"
              :auto.commit.enable false}
        preserve-https (fn [consumer]
                         ;; Annoyingly, the proxy will return an HTTP address for a
                         ;; subscriber even when its running over HTTPS
                         (if (clojure.string/starts-with? url "https")
                           (update consumer :base-uri clojure.string/replace #"^http:" "https:")
                           consumer))]

    (let [response @(handle-proxy-request http/post url headers body)]
      (if (:error response)
        (do (log/infof "rest-proxy create consumer error: %s" (:error response))
            (assoc client :error response))
        (let [{:keys [base-uri instance-id]} (:json-body response)]
          (preserve-https
            (assoc client :base-uri base-uri, :instance-id instance-id)))))))

(defn with-subscription
  [{:keys [base-uri group-id instance-id] :as client} topic-metadata]
  (let [url (format "%s/subscription" base-uri)
        topics (map :topic-name (vals topic-metadata))
        headers {"Accept" (content-types :json)
                 "Content-Type" (content-types :json)}
        body {:topics topics}]
    (let [response @(handle-proxy-request http/post url headers {:topics topics})]
      (if (:error response)
        (do (log/infof "rest-proxy subscription error: %s" (:error response))
            (assoc client :error response))
        (do (log/info "rest-proxy subscription started")
            (assoc client :subscription topics))))))

(defn rest-proxy-poller
  "Returns a function that takes a consumer and puts any messages retrieved
   by polling it onto the supplied `messages` channel"
  [messages]
  (fn [consumer]
    (let [{:keys [base-uri group-id instance-id]} consumer
          url (format "%s/records" base-uri)
          headers {"Accept" (content-types :byte-array)}
          body nil]
      (let [response @(handle-proxy-request http/get url headers body)]
        (when (:error response)
          (log/errorf "rest-proxy fetch error: %s" (:error response)))
        (when-not (:error response)
          (log/info "got rest-proxy poll response")
          (s/put-all! messages (:json-body response)))))))

(defn rest-proxy-subscription
  [config topic-metadata]
  (-> (rest-proxy-client config)
      (with-consumer)
      (with-subscription topic-metadata)))

(defn rest-proxy-consumer
  "Creates an asynchronous Kafka Consumer of all topics defined in the
   supplied `topic-metadata`

   Puts all messages on the channel in the returned response. It is the
   responsibility of the caller to arrange for the read the channel to
   be read by some other process.

   Must be closed with `close-consumer` when no longer required"
  [config topic-metadata deserializers]
  (let [continue?   (atom true)
        xform       (comp
                     #(assoc % :topic (j/reverse-lookup topic-metadata (:topic %)))
                     #(apply-deserializers deserializers %)
                     #(undatafy-record topic-metadata %))
        messages    (s/stream 1 (map xform))
        started?    (promise)
        poll        (rest-proxy-poller messages)]

    {:process (d/loop [consumer (rest-proxy-subscription config topic-metadata)]
                (d/chain (d/future consumer)
                         (fn [consumer]
                           (poll consumer)

                           (when-not (realized? started?)
                             (deliver started? true)
                             (log/infof "started rest-proxy consumer: %s" (proxy-client-info consumer)))

                           (if @continue?
                             (do (poll consumer)
                                 (Thread/sleep 500)
                                 (d/recur consumer))
                             (do
                               (s/close! messages)
                               (destroy-consumer consumer)
                               (log/infof "stopped rest-proxy consumer: %s" (proxy-client-info consumer)))))))
     :started? started?
     :messages messages
     :continue? continue?}))

(defn build-record [m]
  (let [data (-> (select-keys m [:key :value :partition])
                 (assoc :topic (get-in m [:topic :topic-name]))
                 (update :key base64-encode)
                 (update :value base64-encode))]
    (assoc m :data-record data)))

(defn deliver-ack
  "Deliver the `ack` promise with the result of attempting to write to kafka. The
   default command-handler waits for this before on to the next command so the
   test response may indicate the success/failure of each write command."
  [ack]
  (fn [rec-meta ex]
    (when-not (nil? ack)
      (if ex
        (deliver ack {:error :send-error
                      :message (:message ex)})
        (deliver ack (select-keys rec-meta
                                  [:topic :offset :partition
                                   :serialized-key-size
                                   :serialized-value-size]))))))

(defn rest-proxy-producer
  "Creates an asynchronous kafka producer to be used by a test-machine for for
   injecting test messages"
  ([config topics serializers]
   (let [producer       (rest-proxy-client config)
         messages       (s/stream 1 (map (fn [x]
                                           (try
                                             (-> (apply-serializers serializers x)
                                                 (build-record))
                                             (catch Exception e
                                               (let [trace (with-out-str
                                                             (stacktrace/print-cause-trace e))]
                                                 (log/error e trace))
                                               (assoc x :serialization-error e))))))
         _ (log/infof "started rest-proxy producer: %s" producer)
         process (d/loop [message (s/take! messages)]
                   (d/chain message
                     (fn [{:keys [data-record ack serialization-error] :as message}]
                       (cond
                         serialization-error   (do (deliver ack {:error :serialization-error
                                                                 :message (.getMessage serialization-error)})
                                                   (d/recur (s/take! messages)))

                         data-record       (do (log/debug "sending data: " data-record)
                                               (topic-post producer data-record (deliver-ack ack))
                                               (d/recur (s/take! messages)))

                         :else (log/infof "stopped rest-proxy producer: %s" producer)))))]

     {:producer  producer
      :messages  messages
      :process   process})))

(deftransport :confluent-rest-proxy
  [{:keys [config topics]}]
  (let [serdes        (serde-map topics)
        test-consumer (rest-proxy-consumer config topics (get serdes :deserializers))
        test-producer (when @(:started? test-consumer)
                        (rest-proxy-producer config topics (get serdes :serializers)))]
    {:consumer test-consumer
     :producer test-producer
     :serdes serdes
     :topics topics
     :exit-hooks [(fn []
                    (s/close! (:messages test-producer)))
                  (fn []
                    (reset! (:continue? test-consumer) false)
                    @(:process test-consumer)
                    @(:process test-producer))]}))
