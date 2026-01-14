(ns hive.events.async
  "Async extensions for hive-events.

   Provides advanced async patterns beyond basic dispatch:
   - Timeout-wrapped dispatch
   - Retry with backoff
   - Debounced dispatch
   - Event streams (pub/sub)
   - Saga/process manager patterns

   JVM-first with core.async, graceful degradation on CLJS."
  #?(:clj (:require [clojure.core.async :as async
                     :refer [go go-loop <! >! chan timeout alt! pub sub unsub close!]]
                    [hive.events.router :as router])
     :cljs (:require [hive.events.router :as router])))

;; =============================================================================
;; Timeout-wrapped Dispatch

#?(:clj
   (defn dispatch-with-timeout
     "Dispatch event with timeout.

      Returns a channel that receives:
      - {:status :ok :context ctx} on success
      - {:status :timeout} on timeout

      (dispatch-with-timeout [:user/fetch 123] 5000)"
     [event timeout-ms]
     (let [result (chan 1)]
       (go
         (let [dispatch-result (router/dispatch-async event)]
           (alt!
             dispatch-result ([ctx] (>! result {:status :ok :context ctx}))
             (timeout timeout-ms) (>! result {:status :timeout}))))
       result)))

;; =============================================================================
;; Retry with Backoff

#?(:clj
   (defn dispatch-with-retry
     "Dispatch event with exponential backoff retry.

      Options:
      - :max-retries - Maximum retry attempts (default: 3)
      - :initial-delay-ms - First retry delay (default: 100)
      - :max-delay-ms - Maximum retry delay (default: 10000)
      - :backoff-factor - Multiplier per retry (default: 2)
      - :retry-pred - fn [context] -> boolean, whether to retry

      Returns channel with final result or :max-retries-exceeded."
     ([event] (dispatch-with-retry event {}))
     ([event {:keys [max-retries initial-delay-ms max-delay-ms backoff-factor retry-pred]
              :or {max-retries 3
                   initial-delay-ms 100
                   max-delay-ms 10000
                   backoff-factor 2
                   retry-pred (constantly false)}}]
      (let [result (chan 1)]
        (go-loop [attempt 0
                  delay-ms initial-delay-ms]
          (let [ctx (router/dispatch-sync event)]
            (if (and (retry-pred ctx) (< attempt max-retries))
              (do
                (<! (timeout delay-ms))
                (recur (inc attempt)
                       (min (* delay-ms backoff-factor) max-delay-ms)))
              (>! result (if (>= attempt max-retries)
                           {:status :max-retries-exceeded :attempts attempt}
                           {:status :ok :context ctx :attempts (inc attempt)})))))
        result))))

;; =============================================================================
;; Debounced Dispatch

(defonce ^:private debounce-chans (atom {}))

#?(:clj
   (defn dispatch-debounced
     "Dispatch event after delay, canceling previous pending dispatch.

      Useful for search-as-you-type, resize handlers, etc.

      (dispatch-debounced :search [:search/query \"foo\"] 300)"
     [debounce-id event delay-ms]
     ;; Cancel existing debounce
     (when-let [existing (get @debounce-chans debounce-id)]
       (close! existing))
     ;; Create new debounce channel
     (let [cancel-chan (chan)]
       (swap! debounce-chans assoc debounce-id cancel-chan)
       (go
         (let [[_ ch] (async/alts! [(timeout delay-ms) cancel-chan])]
           (when (not= ch cancel-chan)
             (router/dispatch event)
             (swap! debounce-chans dissoc debounce-id)))))))

;; =============================================================================
;; Throttled Dispatch

(defonce ^:private throttle-state (atom {}))

#?(:clj
   (defn dispatch-throttled
     "Dispatch event at most once per interval.

      (dispatch-throttled :scroll [:ui/scroll-position pos] 100)"
     [throttle-id event interval-ms]
     (let [now (System/currentTimeMillis)
           {:keys [last-dispatch]} (get @throttle-state throttle-id)]
       (when (or (nil? last-dispatch)
                 (>= (- now last-dispatch) interval-ms))
         (swap! throttle-state assoc throttle-id {:last-dispatch now})
         (router/dispatch event)))))

;; =============================================================================
;; Event Streams (Pub/Sub)

#?(:clj
   (do
     (defonce ^:private event-pub-chan (chan 1024))
     (defonce ^:private event-pub (pub event-pub-chan first))

     (defn publish-event
       "Publish event to the event stream.

        Events can be subscribed to by topic (first element)."
       [event]
       (async/put! event-pub-chan event))

     (defn subscribe-events
       "Subscribe to events by topic.

        Returns a channel that receives events with the given topic.

        (let [ch (subscribe-events :user/login)]
          (go-loop []
            (when-let [event (<! ch)]
              (println \"Login event:\" event)
              (recur))))"
       [topic]
       (let [ch (chan 32)]
         (sub event-pub topic ch)
         ch))

     (defn unsubscribe-events
       "Unsubscribe channel from topic."
       [topic ch]
       (unsub event-pub topic ch)
       (close! ch))))

;; =============================================================================
;; Saga / Process Manager

#?(:clj
   (defn saga
     "Create a saga (long-running process) that reacts to events.

      A saga is a go-block that:
      1. Subscribes to specified event topics
      2. Runs handler for each event
      3. Can dispatch new events based on state

      Returns a channel that closes the saga when closed.

      (saga [:order/created :payment/completed]
        (fn [event state]
          (case (first event)
            :order/created
            {:state (assoc state :order (second event))
             :dispatch [:payment/initiate (:id (second event))]}

            :payment/completed
            {:state (assoc state :paid? true)
             :dispatch [:fulfillment/start (:order-id state)]})))"
     [topics handler]
     (let [control-chan (chan)
           topic-chans (mapv subscribe-events topics)
           merged (async/merge topic-chans)]
       (go-loop [state {}]
         (let [[v ch] (async/alts! [control-chan merged])]
           (if (= ch control-chan)
             ;; Saga terminated
             (doseq [ch topic-chans]
               (close! ch))
             ;; Process event
             (when v
               (let [{:keys [state dispatch]} (handler v state)]
                 (when dispatch
                   (router/dispatch dispatch))
                 (recur (or state {})))))))
       control-chan)))
