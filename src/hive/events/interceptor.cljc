(ns hive.events.interceptor
  "Interceptor chain implementation.

   Ported from re-frame/interceptor.cljc with JVM compatibility.

   An interceptor is a map with optional keys:
   - :id      - keyword identifier
   - :before  - fn [context] -> context (pre-processing)
   - :after   - fn [context] -> context (post-processing)

   Context is a map containing:
   - :coeffects - input data (including :event and :db)
   - :effects   - output data (side effects to perform)
   - :queue     - interceptors yet to execute
   - :stack     - interceptors already executed (for :after phase)"
  (:require [hive.events.log :as log]
            #?(:clj [clojure.core.async :as async :refer [go <!]])))

(defn ->interceptor
  "Create an interceptor from components.

   (->interceptor
     :id :debug
     :before (fn [ctx] (println (:event (:coeffects ctx))) ctx)
     :after (fn [ctx] (println (:effects ctx)) ctx))"
  [& {:keys [id before after] :as m}]
  (merge {:id (or id :unnamed)
          :before (or before identity)
          :after (or after identity)}
         (dissoc m :id :before :after)))

(defn enqueue
  "Add interceptors to the queue."
  [context interceptors]
  (update context :queue into interceptors))

(defn- invoke-interceptor-fn
  "Invoke interceptor fn, handling nil gracefully."
  [context interceptor direction]
  (if-let [f (get interceptor direction)]
    (f context)
    context))

(defn- invoke-interceptors
  "Walk through interceptors in given direction."
  [context direction]
  (loop [ctx context]
    (let [queue (:queue ctx)]
      (if (empty? queue)
        ctx
        (let [[interceptor & rest-queue] queue
              new-ctx (-> ctx
                          (assoc :queue (vec rest-queue))
                          (update :stack conj interceptor)
                          (invoke-interceptor-fn interceptor direction))]
          (recur new-ctx))))))

(defn- reverse-stack
  "Prepare stack for :after phase (LIFO order)."
  [context]
  (-> context
      (assoc :queue (vec (reverse (:stack context))))
      (assoc :stack [])))

(defn execute
  "Execute interceptor chain synchronously.

   1. Run all :before fns (queue order)
   2. Run all :after fns (reverse order)

   Returns final context with :effects populated."
  [context]
  (-> context
      (invoke-interceptors :before)
      (reverse-stack)
      (invoke-interceptors :after)))

#?(:clj
   (defn execute-async
     "Execute interceptor chain asynchronously.

      Returns a core.async channel that will receive the final context.

      Supports async interceptors that return channels."
     [context]
     (go
       (loop [ctx context
              direction :before
              reversed? false]
         (let [queue (:queue ctx)]
           (if (empty? queue)
             (if (and (= direction :before) (not reversed?))
               ;; Switch to :after phase
               (recur (reverse-stack ctx) :after true)
               ;; Done
               ctx)
             (let [[interceptor & rest-queue] queue
                   new-ctx (-> ctx
                               (assoc :queue (vec rest-queue))
                               (update :stack conj interceptor))
                   result (invoke-interceptor-fn new-ctx interceptor direction)
                   ;; Handle async results
                   final-ctx (if (satisfies? #?(:clj clojure.core.async.impl.protocols/ReadPort
                                                :cljs cljs.core.async.impl.protocols/ReadPort)
                                             result)
                               (<! result)
                               result)]
               (recur final-ctx direction reversed?))))))))

;; =============================================================================
;; Timed Interceptor (JVM only — delegates to hive-weave.timed)
;; =============================================================================

#?(:clj
   (defn ->timed-interceptor
     "Create an interceptor with a timeout budget on :before and/or :after.

      If either phase exceeds timeout-ms, it is cancelled and the context
      passes through unchanged (graceful degradation). This prevents any
      single interceptor from hanging the entire event dispatch chain.

      Usage:
        (->timed-interceptor
          :id :hk/smart-search-enrichment
          :timeout-ms 15000
          :after (fn [ctx] ...expensive enrichment...))

      Delegates to hive-weave.timed/->timed-interceptor via requiring-resolve."
     [& {:keys [_id _before _after _timeout-ms] :as m}]
     (let [f (requiring-resolve 'hive-weave.timed/->timed-interceptor)]
       (apply f (apply concat m)))))

;; =============================================================================
;; Built-in Interceptors

(def debug
  "Log event and effects for debugging."
  (->interceptor
   :id :debug
   :before (fn [ctx]
             (log/debug "event" (get-in ctx [:coeffects :event]))
             ctx)
   :after (fn [ctx]
            (log/debug "effects" (:effects ctx))
            ctx)))

(def trim-v
  "Remove event id from event vector, leaving just args.
   [:user/login {:email ...}] -> [{:email ...}]"
  (->interceptor
   :id :trim-v
   :before (fn [ctx]
             (update-in ctx [:coeffects :event] (comp vec rest)))))

(defn path
  "Interceptor that focuses handler on a path within db.

   (reg-event-db :counter/inc
     [(path [:counters :main])]
     (fn [counter _] (inc counter)))"
  [p]
  (->interceptor
   :id :path
   :before (fn [ctx]
             (let [db (get-in ctx [:coeffects :db])]
               (-> ctx
                   (assoc-in [:coeffects :original-db] db)
                   (assoc-in [:coeffects :db] (get-in db p)))))
   :after (fn [ctx]
            (let [original-db (get-in ctx [:coeffects :original-db])
                  new-value (get-in ctx [:effects :db])]
              (if (contains? (:effects ctx) :db)
                (assoc-in ctx [:effects :db] (assoc-in original-db p new-value))
                ctx)))))
