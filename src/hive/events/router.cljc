(ns hive.events.router
  "Event router - registration and dispatch.

   Ported from re-frame with JVM-first async support via core.async.

   Supports two types of event handlers:
   - reg-event-db: Handler receives db, returns new db
   - reg-event-fx: Handler receives coeffects, returns effects map

   Dispatch modes:
   - dispatch: Async dispatch (queued, processed in order)
   - dispatch-sync: Synchronous dispatch (immediate processing)

   Usage:
     ;; Initialize with app state atom
     (init! (atom {}))

     ;; Register handlers
     (reg-event-db :counter/inc
       (fn [db [_ amount]]
         (update db :count + amount)))

     (reg-event-fx :user/fetch
       (fn [{:keys [db]} [_ user-id]]
         {:db (assoc db :loading? true)
          :http {:url (str \"/users/\" user-id)}}))

     ;; Dispatch events
     (dispatch [:counter/inc 5])
     (dispatch-sync [:counter/inc 1])"
  (:require [hive.events.interceptor :as interceptor]
            [hive.events.fx :as fx]
            [hive.events.cofx :as cofx]
            [hive.events.log :as log]
            #?(:clj [clojure.core.async :as async :refer [go go-loop <! >! chan]])))

;; =============================================================================
;; State

(defonce ^:private app-db (atom {}))
(defonce ^:private event-registry (atom {}))

#?(:clj (defonce ^:private event-queue (atom (chan 1024))))
#?(:clj (defonce ^:private processing? (atom false)))

;; Forward declaration for dispatch-impl registration
(declare dispatch)

(defn init!
  "Initialize the router with an app-db atom.

   Must be called before dispatching events."
  [db-atom]
  (reset! app-db db-atom)

  ;; Register :db effect handler
  (fx/reg-fx :db
             (fn [new-db]
               (reset! db-atom new-db)))

  ;; Inject dispatch function for :dispatch effects
  ;; Using a wrapper to avoid forward reference issues
  (fx/reg-fx :dispatch-impl (fn [event] (dispatch event))))

(defn get-app-db
  "Get the current app-db atom."
  []
  @app-db)

;; =============================================================================
;; Handler Registration

(defn- flatten-interceptors
  "Flatten and normalize interceptor collection."
  [interceptors]
  (->> interceptors
       flatten
       (remove nil?)
       (map (fn [i]
              (if (fn? i)
                (interceptor/->interceptor :id :inline :before i)
                i)))
       vec))

(defn reg-event-fx
  "Register an event handler that returns effects map.

   Handler signature: (fn [coeffects event] effects-map)

   Coeffects contain :db, :event, and any injected coeffects.
   Effects map can contain :db, :dispatch, and custom effects."
  ([id handler]
   (reg-event-fx id [] handler))
  ([id interceptors handler]
   (let [interceptors (flatten-interceptors interceptors)
         handler-interceptor (interceptor/->interceptor
                              :id :handler
                              :before (fn [ctx]
                                        (let [coeffects (:coeffects ctx)
                                              effects (handler coeffects (:event coeffects))]
                                          (assoc ctx :effects effects))))]
     (swap! event-registry assoc id
            {:interceptors (conj interceptors handler-interceptor)
             :handler handler}))))

(defn reg-event-db
  "Register an event handler that returns new db value.

   Handler signature: (fn [db event] new-db)

   Syntactic sugar for reg-event-fx handlers that only update :db."
  ([id handler]
   (reg-event-db id [] handler))
  ([id interceptors handler]
   (reg-event-fx id interceptors
                 (fn [coeffects event]
                   {:db (handler (:db coeffects) event)}))))

(defn clear-event
  "Clear event handler. If no id provided, clear all."
  ([]
   (reset! event-registry {}))
  ([id]
   (swap! event-registry dissoc id)))

;; =============================================================================
;; Dispatch

(defn- create-context
  "Create initial context for event processing."
  [event]
  {:coeffects {:event event
               :db @@app-db}
   :effects {}
   :queue []
   :stack []})

(defn- process-event
  "Process a single event through its interceptor chain."
  [event]
  (let [event-id (first event)]
    (if-let [{:keys [interceptors]} (get @event-registry event-id)]
      (let [context (-> (create-context event)
                        (interceptor/enqueue interceptors)
                        (interceptor/execute))]
        (fx/do-fx (:effects context))
        context)
      (do
        (log/warn "no handler for event" event-id)
        nil))))

(defn dispatch-sync
  "Dispatch event synchronously.

   Processes event immediately, blocking until complete.
   Use for events that must complete before continuing."
  [event]
  (process-event event))

#?(:clj
   (defn- start-event-loop!
     "Start the async event processing loop."
     []
     (when (compare-and-set! processing? false true)
       (let [q @event-queue]
         (go-loop []
           (if-let [event (<! q)]
             (do
               (try
                 (process-event event)
                 (catch Exception e
                   (log/error "error processing event" (first event) e)))
               (recur))
             ;; Channel closed — mark processing as stopped
             (reset! processing? false)))))))

(defn dispatch
  "Dispatch event asynchronously.

   On JVM: Queues event for processing in order
   On JS: Uses setTimeout for async behavior"
  [event]
  #?(:clj (do
            (start-event-loop!)
            (async/put! @event-queue event))
     :cljs (js/setTimeout #(process-event event) 0)))

;; =============================================================================
;; Async Extensions (JVM only)

#?(:clj
   (defn dispatch-async
     "Dispatch event and return a channel with the result.

      Useful for waiting on event processing completion."
     [event]
     (let [result-chan (chan 1)]
       (go
         (let [context (process-event event)]
           (>! result-chan context)))
       result-chan)))

#?(:clj
   (defn dispatch-all
     "Dispatch multiple events and wait for all to complete.

      Returns a channel with all contexts."
     [events]
     (async/merge (map dispatch-async events))))

#?(:clj
   (defn stop!
     "Stop the event processing loop and reset the queue.

      Closes the current event-queue channel (causing the go-loop to terminate),
      creates a fresh channel, and resets the processing flag.

      After calling stop!, you can restart by calling init! and dispatching events."
     []
     (when (compare-and-set! processing? true false)
       (async/close! @event-queue))
     (reset! event-queue (chan 1024))))
