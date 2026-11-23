(ns hive.events.cloudevents
  "CloudEvents 1.0 context attributes and the NATS binary content mode codec.
   Pure, cljc.

   Attributes are a map with unqualified keyword keys named after the
   CloudEvents attribute (:specversion :id :source :type :time ...). Every value
   is its canonical CloudEvents string, except the one hive extension
   :hiveschemaversion, a positive Integer.

   In binary content mode every attribute travels as its own NATS header named
   `ce-<attribute>`, the value percent-encoded as section 3.1.3.2 of the
   CloudEvents NATS protocol binding prescribes. The event data is the message
   body and is never read or written here.

   Spec: https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md
   Binding: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md

   Every fn that can refuse returns a hive-dsl Result: {:ok v} or {:error kw ...}."
  (:require [clojure.string :as str]
            [malli.core :as m]
            [malli.error :as me]
            [hive-dsl.result :as r]))

;; ---------------------------------------------------------------------------
;; Attribute names and values

(def spec-version
  "The only CloudEvents specversion this codec speaks."
  "1.0")

(def schema-version
  "The value of the hiveschemaversion extension stamped by `envelope`."
  1)

(def header-prefix "ce-")

(def ^:private attribute-name-re #"[a-z0-9]{1,20}")

(defn attribute-name?
  "True when `s` is a legal CloudEvents attribute name: lower-case ASCII letters
   and digits, length 1 to 20 (spec: Attribute Naming Convention)."
  [s]
  (and (string? s) (some? (re-matches attribute-name-re s))))

(defn- control-char?
  "U+0000-U+001F and U+007F-U+009F are not allowed in a CloudEvents String."
  [c]
  (let [n #?(:clj (int c) :cljs (.charCodeAt c 0))]
    (or (<= 0 n 0x1F) (<= 0x7F n 0x9F))))

(defn- ce-string?
  [s]
  (and (string? s) (not (some control-char? s))))

(def ^:private rfc3339-re
  #"(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(\.\d+)?([Zz]|[+-](\d{2}):(\d{2}))")

(defn- parse-int10 [s]
  #?(:clj (Long/parseLong s) :cljs (js/parseInt s 10)))

(defn rfc3339?
  "True when `s` is an RFC 3339 date-time (section 5.6), with field ranges
   checked."
  [s]
  (boolean
   (when (string? s)
     (when-let [[_ _ mo d h mi sec _ _ oh om] (re-matches rfc3339-re s)]
       (and (<= 1 (parse-int10 mo) 12)
            (<= 1 (parse-int10 d) 31)
            (<= 0 (parse-int10 h) 23)
            (<= 0 (parse-int10 mi) 59)
            (<= 0 (parse-int10 sec) 60)
            (or (nil? oh) (and (<= 0 (parse-int10 oh) 23)
                               (<= 0 (parse-int10 om) 59))))))))

(def AttributeName
  [:and :keyword
   [:fn {:error/message "attribute name must be [a-z0-9]{1,20}"}
    (fn [k] (and (nil? (namespace k)) (attribute-name? (name k))))]])

(def CEString
  [:and :string
   [:fn {:error/message "must not contain U+0000-U+001F or U+007F-U+009F"}
    ce-string?]])

(def NonEmptyCEString [:and CEString [:string {:min 1}]])

(def Timestamp
  [:and :string [:fn {:error/message "must be an RFC 3339 date-time"} rfc3339?]])

(def Int32Pos [:int {:min 1 :max 2147483647}])

(def Attributes
  "CloudEvents 1.0 context attributes, open for extensions. The required four,
   the optional ones hive uses, and the hiveschemaversion extension. Any other
   key is an extension: its name must be a legal attribute name and its value a
   CloudEvents String."
  [:and
   [:map
    [:specversion [:enum spec-version]]
    [:id NonEmptyCEString]
    [:source NonEmptyCEString]
    [:type NonEmptyCEString]
    [:time {:optional true} Timestamp]
    [:datacontenttype {:optional true} NonEmptyCEString]
    [:dataschema {:optional true} NonEmptyCEString]
    [:subject {:optional true} NonEmptyCEString]
    [:hiveschemaversion {:optional true} Int32Pos]]
   [:map-of AttributeName [:or CEString Int32Pos]]])

(def ^:private validator (m/validator Attributes))
(def ^:private explainer (m/explainer Attributes))

(def ^:private integer-attributes #{:hiveschemaversion})

(defn- bad-names
  [attrs]
  (seq (remove (fn [k] (and (keyword? k) (nil? (namespace k))
                            (attribute-name? (name k))))
               (keys attrs))))

(defn validate
  "Result of checking `attrs` against `Attributes`. A key that is not a legal
   attribute name is refused as :cloudevents/invalid-attribute-name."
  [attrs]
  (cond
    (not (map? attrs))
    (r/err :cloudevents/invalid-attributes {:explain "attributes must be a map"})

    (bad-names attrs)
    (r/err :cloudevents/invalid-attribute-name {:names (vec (bad-names attrs))})

    (validator attrs)
    (r/ok attrs)

    :else
    (r/err :cloudevents/invalid-attributes
           {:explain (me/humanize (explainer attrs))})))

;; ---------------------------------------------------------------------------
;; Percent-encoding (binding section 3.1.3.2)

(defn- char-code [s i]
  #?(:clj (int (.charAt ^String s (int i))) :cljs (.charCodeAt s i)))

(defn- code-points
  "The Unicode code points of `s`; a surrogate pair is one code point, a lone
   surrogate becomes U+FFFD."
  [s]
  (let [n (count s)]
    (loop [i 0 acc (transient [])]
      (if (>= i n)
        (persistent! acc)
        (let [c (char-code s i)]
          (cond
            (and (<= 0xD800 c 0xDBFF) (< (inc i) n)
                 (<= 0xDC00 (char-code s (inc i)) 0xDFFF))
            (recur (+ i 2)
                   (conj! acc (+ 0x10000
                                 (bit-shift-left (- c 0xD800) 10)
                                 (- (char-code s (inc i)) 0xDC00))))

            (<= 0xD800 c 0xDFFF)
            (recur (inc i) (conj! acc 0xFFFD))

            :else
            (recur (inc i) (conj! acc c))))))))

(defn- utf8-bytes
  [cp]
  (cond
    (< cp 0x80)    [cp]
    (< cp 0x800)   [(bit-or 0xC0 (bit-shift-right cp 6))
                    (bit-or 0x80 (bit-and cp 0x3F))]
    (< cp 0x10000) [(bit-or 0xE0 (bit-shift-right cp 12))
                    (bit-or 0x80 (bit-and (bit-shift-right cp 6) 0x3F))
                    (bit-or 0x80 (bit-and cp 0x3F))]
    :else          [(bit-or 0xF0 (bit-shift-right cp 18))
                    (bit-or 0x80 (bit-and (bit-shift-right cp 12) 0x3F))
                    (bit-or 0x80 (bit-and (bit-shift-right cp 6) 0x3F))
                    (bit-or 0x80 (bit-and cp 0x3F))]))

(def ^:private hex-digits "0123456789ABCDEF")

(defn- pct [b]
  (str "%" (nth hex-digits (bit-shift-right b 4)) (nth hex-digits (bit-and b 0xF))))

(defn- cp->str [cp]
  #?(:clj  (String. (Character/toChars (int cp)))
     :cljs (.fromCodePoint js/String cp)))

(defn- needs-encoding?
  "Space, double-quote, percent, and anything outside printable ASCII."
  [cp]
  (or (= cp 0x20) (= cp 0x22) (= cp 0x25) (< cp 0x21) (> cp 0x7E)))

(defn percent-encode
  "Header value for the canonical string `s`, percent-encoded with upper-case
   hex as the binding requires."
  [s]
  (apply str (map (fn [cp]
                    (if (needs-encoding? cp)
                      (apply str (map pct (utf8-bytes cp)))
                      (cp->str cp)))
                  (code-points s))))

(defn- hex-val [c]
  (let [n (char-code (str c) 0)]
    (cond (<= 48 n 57)  (- n 48)
          (<= 65 n 70)  (- n 55)
          (<= 97 n 102) (- n 87))))

(defn- decode-utf8
  "Strictly decode the byte vector `bs` to a string. Overlong forms, surrogate
   code points, values past U+10FFFF and broken sequences are refused."
  [bs]
  (let [n (count bs)
        cont? (fn [i] (and (< i n) (= 0x80 (bit-and (nth bs i) 0xC0))))]
    (loop [i 0 out []]
      (if (>= i n)
        (r/ok (apply str out))
        (let [b0 (nth bs i)
              [len init minimum] (cond (< b0 0x80)            [1 b0 0]
                                       (= 0xC0 (bit-and b0 0xE0)) [2 (bit-and b0 0x1F) 0x80]
                                       (= 0xE0 (bit-and b0 0xF0)) [3 (bit-and b0 0x0F) 0x800]
                                       (= 0xF0 (bit-and b0 0xF8)) [4 (bit-and b0 0x07) 0x10000])]
          (if (or (nil? len) (not (every? cont? (range (inc i) (+ i len)))))
            (r/err :cloudevents/invalid-utf8 {:at i})
            (let [cp (reduce (fn [acc j] (bit-or (bit-shift-left acc 6)
                                                 (bit-and (nth bs j) 0x3F)))
                             init (range (inc i) (+ i len)))]
              (if (or (< cp minimum) (<= 0xD800 cp 0xDFFF) (> cp 0x10FFFF))
                (r/err :cloudevents/invalid-utf8 {:at i})
                (recur (+ i len) (conj out (cp->str cp)))))))))))

(defn percent-decode
  "Result of one round of percent-decoding the header value `s`. Lower-case hex
   and needlessly encoded characters are accepted; invalid UTF-8 is refused."
  [s]
  (let [n (count s)]
    (loop [i 0 bytes [] out []]
      (cond
        (>= i n)
        (r/let-ok [tail (decode-utf8 bytes)]
          (r/ok (apply str (conj out tail))))

        (= \% (nth s i))
        (let [hi (some-> (get s (+ i 1)) hex-val)
              lo (some-> (get s (+ i 2)) hex-val)]
          (if (and hi lo)
            (recur (+ i 3) (conj bytes (+ (* 16 hi) lo)) out)
            (r/err :cloudevents/bad-percent-encoding {:value s :at i})))

        :else
        (let [r (decode-utf8 bytes)]
          (if (r/ok? r)
            (recur (inc i) [] (conj out (:ok r) (str (nth s i))))
            r))))))

(defn- unquote-value
  "Trim optional whitespace, then remove RFC 7230 quoted-string quoting:
   a double quote toggles quoting, a backslash inside quotes escapes the next
   character."
  [s]
  (let [s (str/replace s #"^[ \t]+|[ \t]+$" "")
        n (count s)]
    (loop [i 0 quoted? false out []]
      (if (>= i n)
        (apply str out)
        (let [c (nth s i)]
          (cond
            (= c \") (recur (inc i) (not quoted?) out)
            (and quoted? (= c \\) (< (inc i) n)) (recur (+ i 2) quoted? (conj out (nth s (inc i))))
            :else (recur (inc i) quoted? (conj out c))))))))

(defn decode-header-value
  "Result of turning a received header value into the attribute's canonical
   string: unquote, then one round of percent-decoding."
  [s]
  (percent-decode (unquote-value s)))

;; ---------------------------------------------------------------------------
;; Binary content mode

(defn- canonical-string [k v]
  (if (contains? integer-attributes k) (str v) v))

(defn ->headers
  "Result of mapping valid `attrs` to NATS binary content mode headers: one
   header `ce-<name>` per attribute, lower-case names, percent-encoded values.
   datacontenttype travels as ce-datacontenttype, as section 3.1.3.1 of the
   binding requires. With opts {:content-type? true} it is also mirrored to
   the `content-type` header for consumers that are not CloudEvents aware."
  ([attrs] (->headers attrs nil))
  ([attrs {:keys [content-type?]}]
   (r/let-ok [attrs (validate attrs)]
     (r/ok (cond-> (into {}
                         (map (fn [[k v]]
                                [(str header-prefix (name k))
                                 (percent-encode (canonical-string k v))]))
                         attrs)
             (and content-type? (:datacontenttype attrs))
             (assoc "content-type" (percent-encode (:datacontenttype attrs))))))))

(defn- header-string [v]
  (cond (string? v) v
        (sequential? v) (first v)
        :else (str v)))

(defn- parse-attribute [k s]
  (if (contains? integer-attributes k)
    (if (re-matches #"-?[0-9]{1,10}" s)
      (r/ok (parse-int10 s))
      (r/err :cloudevents/invalid-attributes {:explain {k ["must be an Integer"]}}))
    (r/ok s)))

(defn <-headers
  "Result of reading the CloudEvents attributes out of NATS headers `m`
   (header name to value, or to a sequence of values, first one wins). Header
   names are matched case-insensitively. Headers without the ce- prefix are
   ignored, except `content-type`, which supplies datacontenttype when no
   ce-datacontenttype header is present. A ce- header whose suffix is not a
   legal attribute name is refused. The attributes are validated."
  [m]
  (let [entries (map (fn [[k v]] [(str/lower-case (name k)) (header-string v)]) m)
        ce      (filter #(str/starts-with? (first %) header-prefix) entries)
        ctype   (some (fn [[k v]] (when (= k "content-type") v)) entries)
        names   (map #(subs (first %) (count header-prefix)) ce)]
    (if-let [bad (seq (remove attribute-name? names))]
      (r/err :cloudevents/invalid-attribute-name {:names (vec bad)})
      (r/let-ok [attrs (reduce (fn [acc [hk hv]]
                                 (let [k (keyword (subs hk (count header-prefix)))
                                       res (r/let-ok [s (decode-header-value hv)]
                                             (parse-attribute k s))]
                                   (if (r/ok? res)
                                     (r/ok (assoc (:ok acc) k (:ok res)))
                                     (reduced res))))
                               (r/ok {}) ce)
                 attrs (if (and ctype (not (contains? attrs :datacontenttype)))
                         (r/let-ok [s (decode-header-value ctype)]
                           (r/ok (assoc attrs :datacontenttype s)))
                         (r/ok attrs))]
        (validate attrs)))))

;; ---------------------------------------------------------------------------
;; Envelope

(defn- pad [width n]
  (let [s (str n)]
    (str (apply str (repeat (- width (count s)) "0")) s)))

(defn- civil-from-days
  "[year month day] of `z` days since 1970-01-01 (Howard Hinnant's algorithm)."
  [z]
  (let [z   (+ z 719468)
        era (quot (if (>= z 0) z (- z 146096)) 146097)
        doe (- z (* era 146097))
        yoe (quot (- doe (quot doe 1460) (- (quot doe 36524)) (quot doe 146096)) 365)
        y   (+ yoe (* era 400))
        doy (- doe (- (+ (* 365 yoe) (quot yoe 4)) (quot yoe 100)))
        mp  (quot (+ (* 5 doy) 2) 153)
        d   (inc (- doy (quot (+ (* 153 mp) 2) 5)))
        mo  (if (< mp 10) (+ mp 3) (- mp 9))]
    [(if (<= mo 2) (inc y) y) mo d]))

(defn epoch-millis->rfc3339
  "RFC 3339 UTC timestamp of `ms` milliseconds since the epoch, with
   millisecond precision: 2018-04-05T03:56:24.000Z. Identical on clj and cljs."
  [ms]
  (let [days (#?(:clj Math/floorDiv :cljs (fn [a b] (Math/floor (/ a b)))) (long ms) 86400000)
        rem  (- (long ms) (* days 86400000))
        [y mo d] (civil-from-days (long days))
        h   (quot rem 3600000)
        mi  (quot (mod rem 3600000) 60000)
        s   (quot (mod rem 60000) 1000)
        mil (mod rem 1000)]
    (str (pad 4 y) "-" (pad 2 mo) "-" (pad 2 d) "T"
         (pad 2 h) ":" (pad 2 mi) ":" (pad 2 s) "." (pad 3 mil) "Z")))

(defn envelope
  "Result of building validated attributes for one event. Nothing is read from
   the environment: the id and the clock are passed in.

   `m` takes :type and :source (required); :id, a string or a 0-arity fn
   returning one (required); :clock, a 0-arity fn returning epoch millis, or
   :time, an RFC 3339 string; optional :subject, :datacontenttype, :dataschema,
   :hiveschemaversion (default `schema-version`) and :extensions, a map of
   further attribute keyword to string."
  [{:keys [id clock time type source subject datacontenttype dataschema
           hiveschemaversion extensions]}]
  (let [id   (if (fn? id) (id) id)
        time (or time (when clock (epoch-millis->rfc3339 (clock))))]
    (validate
     (cond-> (merge extensions
                    {:specversion       spec-version
                     :id                id
                     :source            source
                     :type              type
                     :hiveschemaversion (or hiveschemaversion schema-version)})
       time            (assoc :time time)
       subject         (assoc :subject subject)
       datacontenttype (assoc :datacontenttype datacontenttype)
       dataschema      (assoc :dataschema dataschema)))))
