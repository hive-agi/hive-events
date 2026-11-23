(ns hive.events.cloudevents-test
  "CloudEvents 1.0 attributes and the NATS binary content mode header codec."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive.events.cloudevents :as ce]))

;; ---------------------------------------------------------------------------
;; Generators

(def gen-code-point
  "Any Unicode scalar value a CloudEvents String may hold: no C0/C1 controls,
   no DEL, no lone surrogates; astral planes included."
  (gen/frequency [[6 (gen/choose 0x20 0x7E)]
                  [2 (gen/choose 0xA0 0xD7FF)]
                  [1 (gen/choose 0xE000 0xFFFD)]
                  [1 (gen/choose 0x10000 0x10FFFF)]]))

(def gen-ce-string
  (gen/fmap #(apply str (map (fn [cp] (#'ce/cp->str cp)) %))
            (gen/vector gen-code-point 0 24)))

(def gen-ne-ce-string (gen/such-that seq gen-ce-string 100))

(def gen-attribute-name
  (gen/fmap #(apply str %)
            (gen/vector (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789")) 1 20)))

(def reserved
  #{"specversion" "id" "source" "type" "time" "datacontenttype" "dataschema"
    "subject" "hiveschemaversion" "data"})

(def gen-extensions
  (gen/map (gen/fmap keyword (gen/such-that (complement reserved) gen-attribute-name 100))
           gen-ce-string
           {:max-elements 4}))

(def gen-time
  (gen/fmap ce/epoch-millis->rfc3339
            (gen/large-integer* {:min -62135596800000 :max 253402300799999})))

(defn- optional [k g] (gen/one-of [(gen/return nil) (gen/fmap (fn [v] [k v]) g)]))

(def gen-attributes
  (gen/let [required (gen/hash-map :id gen-ne-ce-string
                                   :source gen-ne-ce-string
                                   :type gen-ne-ce-string)
            opts (gen/tuple (optional :time gen-time)
                            (optional :datacontenttype gen-ne-ce-string)
                            (optional :dataschema gen-ne-ce-string)
                            (optional :subject gen-ne-ce-string)
                            (optional :hiveschemaversion (gen/choose 1 2147483647)))
            ext gen-extensions]
    (merge ext required {:specversion "1.0"} (into {} (remove nil?) opts))))

(def gen-bad-name
  "Names outside [a-z0-9]{1,20} that stay illegal after header names are
   lower-cased: empty, too long, or holding a character that is not a letter
   or digit."
  (gen/one-of
   [(gen/return "")
    (gen/fmap #(apply str %)
              (gen/vector (gen/elements (seq "abc123")) 21 40))
    (gen/let [pre gen-attribute-name
              bad (gen/elements (seq "_-.:/ é$"))
              post gen-attribute-name]
      (str pre bad post))]))

;; ---------------------------------------------------------------------------
;; Properties

(defspec headers-round-trip-is-identity 300
  (prop/for-all [attrs gen-attributes]
    (let [hs (ce/->headers attrs)]
      (and (contains? hs :ok)
           (= {:ok attrs} (ce/<-headers (:ok hs)))))))

(defspec header-values-are-printable-ascii 300
  (prop/for-all [attrs gen-attributes]
    (every? #(re-matches #"[\x21-\x7E]*" %) (vals (:ok (ce/->headers attrs))))))

(defspec header-names-are-matched-case-insensitively 100
  (prop/for-all [attrs gen-attributes]
    (= {:ok attrs}
       (ce/<-headers (update-keys (:ok (ce/->headers attrs)) str/upper-case)))))

(defspec illegal-attribute-name-is-refused 300
  (prop/for-all [bad gen-bad-name
                 attrs gen-attributes]
    (let [hs (:ok (ce/->headers attrs))]
      (and (= :cloudevents/invalid-attribute-name
              (:error (ce/->headers (assoc attrs (keyword bad) "x"))))
           (= :cloudevents/invalid-attribute-name
              (:error (ce/<-headers (assoc hs (str "ce-" bad) "x"))))))))

(defspec upper-case-attribute-key-is-refused 100
  (prop/for-all [n gen-attribute-name
                 attrs gen-attributes]
    (let [upper (str/upper-case n)]
      (or (= upper n)
          (= :cloudevents/invalid-attribute-name
             (:error (ce/validate (assoc attrs (keyword upper) "x"))))))))

;; ---------------------------------------------------------------------------
;; Examples

(def nats-binding-example-headers
  "Section 3.1.4 of the CloudEvents NATS protocol binding, binary content mode."
  {"ce-specversion"     "1.0"
   "ce-type"            "com.example.someevent"
   "ce-time"            "2018-04-05T03:56:24Z"
   "ce-id"              "1234-1234-1234"
   "ce-source"          "/mycontext/subcontext"
   "ce-datacontenttype" "application/json"})

(def nats-binding-example-attributes
  {:specversion     "1.0"
   :type            "com.example.someevent"
   :time            "2018-04-05T03:56:24Z"
   :id              "1234-1234-1234"
   :source          "/mycontext/subcontext"
   :datacontenttype "application/json"})

(deftest nats-binding-binary-mode-example
  (testing "the binding's example headers decode to the event's attributes"
    (is (= {:ok nats-binding-example-attributes}
           (ce/<-headers nats-binding-example-headers))))
  (testing "the event's attributes encode to exactly the binding's headers"
    (is (= {:ok nats-binding-example-headers}
           (ce/->headers nats-binding-example-attributes))))
  (testing "datacontenttype may also be mirrored to content-type on request"
    (is (= "application/json"
           (get (:ok (ce/->headers nats-binding-example-attributes {:content-type? true}))
                "content-type")))))

(deftest content-type-header-supplies-datacontenttype
  (let [hs (dissoc nats-binding-example-headers "ce-datacontenttype")]
    (is (= {:ok nats-binding-example-attributes}
           (ce/<-headers (assoc hs "Content-Type" "application/json"))))
    (testing "ce-datacontenttype wins over content-type"
      (is (= "application/json"
             (:datacontenttype (:ok (ce/<-headers (assoc nats-binding-example-headers
                                                         "content-type" "text/plain"))))))))
  (testing "headers outside the ce- set are ignored"
    (is (= {:ok (dissoc nats-binding-example-attributes :datacontenttype)}
           (ce/<-headers (-> nats-binding-example-headers
                             (dissoc "ce-datacontenttype")
                             (assoc "traceparent" "00-abc" "Nats-Msg-Id" "z")))))))

(deftest percent-encoding-follows-the-binding
  (testing "the binding's own example"
    (is (= "Euro%20%E2%82%AC%20%F0%9F%98%80" (ce/percent-encode "Euro € 😀")))
    (is (= {:ok "Euro € 😀"} (ce/percent-decode "Euro%20%E2%82%AC%20%F0%9F%98%80"))))
  (testing "space, double quote and percent are encoded"
    (is (= "a%20%22b%22%25" (ce/percent-encode "a \"b\"%"))))
  (testing "lower-case hex and needless encoding are accepted on decode"
    (is (= {:ok "€A"} (ce/percent-decode "%e2%82%ac%41"))))
  (testing "an overlong UTF-8 sequence is refused"
    (is (= :cloudevents/invalid-utf8 (:error (ce/percent-decode "%C0%A0")))))
  (testing "a truncated escape is refused"
    (is (= :cloudevents/bad-percent-encoding (:error (ce/percent-decode "ab%4")))))
  (testing "quoted-string values from older senders are unquoted first"
    (is (= {:ok "a b\"c"} (ce/decode-header-value " \"a b\\\"c\" ")))))

(deftest attribute-validation
  (testing "the four required attributes"
    (doseq [k [:specversion :id :source :type]]
      (is (= :cloudevents/invalid-attributes
             (:error (ce/validate (dissoc nats-binding-example-attributes k)))))))
  (testing "specversion is 1.0 only"
    (is (ce/validate (assoc nats-binding-example-attributes :specversion "1.0")))
    (is (= :cloudevents/invalid-attributes
           (:error (ce/validate (assoc nats-binding-example-attributes :specversion "0.3"))))))
  (testing "time must be RFC 3339"
    (is (contains? (ce/validate (assoc nats-binding-example-attributes
                                       :time "2018-04-05T03:56:24.123+03:00"))
                   :ok))
    (doseq [t ["2018-04-05" "2018-13-05T03:56:24Z" "2018-04-05 03:56:24Z" "yesterday"]]
      (is (= :cloudevents/invalid-attributes
             (:error (ce/validate (assoc nats-binding-example-attributes :time t))))
          t)))
  (testing "control characters are refused in String attributes"
    (is (= :cloudevents/invalid-attributes
           (:error (ce/validate (assoc nats-binding-example-attributes :subject "a\nb"))))))
  (testing "hiveschemaversion is a positive Integer and travels as its decimal string"
    (is (= "7" (get (:ok (ce/->headers (assoc nats-binding-example-attributes
                                              :hiveschemaversion 7)))
                    "ce-hiveschemaversion")))
    (is (= :cloudevents/invalid-attributes
           (:error (ce/<-headers (assoc nats-binding-example-headers
                                        "ce-hiveschemaversion" "one")))))))

(deftest envelope-takes-id-and-clock-as-arguments
  (testing "id and clock are passed in, never read from the environment"
    (is (= {:ok {:specversion       "1.0"
                 :id                "evt-1"
                 :source            "/hive/node-a"
                 :type              "hive.shout"
                 :time              "2018-04-05T03:56:24.000Z"
                 :subject           "ling-7"
                 :hiveschemaversion 1}}
           (ce/envelope {:id      (constantly "evt-1")
                         :clock   (constantly 1522900584000)
                         :type    "hive.shout"
                         :source  "/hive/node-a"
                         :subject "ling-7"}))))
  (testing "an explicit time and extensions pass through"
    (is (= {:ok {:specversion "1.0" :id "x" :source "/s" :type "t"
                 :time "2018-04-05T03:56:24Z" :hiveschemaversion 2 :traceflag "on"}}
           (ce/envelope {:id "x" :time "2018-04-05T03:56:24Z" :type "t" :source "/s"
                         :hiveschemaversion 2 :extensions {:traceflag "on"}}))))
  (testing "a missing required attribute is refused"
    (is (= :cloudevents/invalid-attributes
           (:error (ce/envelope {:id "x" :type "t"}))))))

(deftest epoch-millis-formatting
  (is (= "1970-01-01T00:00:00.000Z" (ce/epoch-millis->rfc3339 0)))
  (is (= "1969-12-31T23:59:59.999Z" (ce/epoch-millis->rfc3339 -1)))
  (is (= "2000-02-29T00:00:00.000Z" (ce/epoch-millis->rfc3339 951782400000)))
  (is (= "9999-12-31T23:59:59.999Z" (ce/epoch-millis->rfc3339 253402300799999))))
