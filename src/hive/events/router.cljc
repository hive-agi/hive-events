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
            #?(:clj [clojure.core.async :as async :refer [go go-loop <! >! chan]])
            [hive.events.observer :as observer]))

;; =============================================================================
;; State

;; Outer atom holds an inner atom (atom-of-atom). Default to a fresh inner
;; atom so `dispatch`/`dispatch-sync` work without an explicit `init!` call;
;; `init!` can still swap in a caller-provided atom.
(defonce ^:private app-db (atom (atom {})))
(defonce ^:private event-registry (atom {}))

#?(:clj (defonce ^:private event-queue (atom (chan 1024))))
#?(:clj (defonce ^:private processing? (atom false)))
;; Promise delivered by the go-loop when it terminates. `stop!` waits on
;; this to ensure the previous loop has exited before installing a fresh
;; channel, preventing stop/start races.
#?(:clj (defonce ^:private loop-exit (atom nil)))

;; Forward declaration for dispatch-impl registration
(declare dispatch)

;; Register default :db and :dispatch-impl fx handlers against the default
;; inner atom so dispatch works without requiring an explicit `init!`.
;; These are a fallback: only install them if nothing else has already
;; registered a handler, so that `init!`-supplied handlers win and we don't
;; emit spurious "overwriting fx handler" warnings on load order shuffles.
;; `init!` itself explicitly replaces these (see below).
(defonce ^:private _default-fx-registration
  (do
    (when-not (fx/get-fx :db)
      (fx/reg-fx :db
                 (fn [new-db]
                   (reset! @app-db new-db))))
    (when-not (fx/get-fx :dispatch-impl)
      (fx/reg-fx :dispatch-impl (fn [event] (dispatch event))))
    ::registered))

(defn init!
  "Initialize the router with an app-db atom.

   May be called to install a caller-supplied app-db; dispatch also works
   without calling init! thanks to the default registrations above."
  [db-atom]
  (reset! app-db db-atom)

  ;; Replace :db and :dispatch-impl handlers. The defaults (or a previous
  ;; init!) may already have registered these; unreg first so we silently
  ;; overwrite without emitting `overwriting fx handler` warnings — the
  ;; replacement is the explicit intent of calling init!.
  (fx/unreg-fx :db)
  (fx/reg-fx :db
             (fn [new-db]
               (reset! db-atom new-db)))

  ;; Inject dispatch function for :dispatch effects
  ;; Using a wrapper to avoid forward reference issues
  (fx/unreg-fx :dispatch-impl)
  (fx/reg-fx :dispatch-impl (fn [event] (dispatch event))))

(defn get-app-db
  "Return the inner db atom currently held by the router.

   `app-db` is internally an atom-of-atom (an outer pointer containing the
   caller-supplied inner atom, or the default inner atom when `init!` has
   not been called). This returns the INNER atom — deref it to read the
   current db value, or `reset!` / `swap!` it to mutate state.

   Note: this is a snapshot of the pointer at call time; if `init!` is
   later called with a different atom, the returned handle will be stale."
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
  "Process a single event through its interceptor chain, then notify observers."
  [event]
  (let [event-id (first event)]
    (if-let [{:keys [interceptors]} (get @event-registry event-id)]
      (let [context (-> (create-context event)
                        (interceptor/enqueue interceptors)
                        (interceptor/execute))]
        (fx/do-fx (:effects context))
        (observer/notify! event-id context)
        context)
      (do
        (log/warn "no handler for event" event-id)
        (observer/notify! event-id (create-context event))
        nil))))

(defn dispatch-sync
  "Dispatch event synchronously.

   Processes event immediately, blocking until complete.
   Use for events that must complete before continuing."
  [event]
  (process-event event))

#?(:clj
   (defn- start-event-loop!
     "Start the async event processing loop.

      Items on the queue are maps with ::event and ::bindings keys.
      The caller's thread bindings are restored on the loop thread
      before `process-event` runs so dynamic-var isolation hooks
      (e.g. test *test-conn*) take effect inside event handlers."
     []
     (when (compare-and-set! processing? false true)
       (let [q @event-queue
             exited (promise)]
         (reset! loop-exit exited)
         (go-loop []
           (if-let [item (<! q)]
             (do
               (try
                 (let [event    (::event item)
                       bindings (::bindings item)]
                   (with-bindings* (or bindings {})
                     (fn [] (process-event event))))
                 (catch Throwable e
                   (log/error "error processing event"
                              (first (::event item)) e)))
               (recur))
             ;; Channel closed — mark processing as stopped and signal exit
             (do
               (reset! processing? false)
               (deliver exited :exited))))))))

(defn dispatch
  "Dispatch event asynchronously.

   On JVM: Queues event for processing in order. Captures the caller's
   thread bindings (via `get-thread-bindings`) so that dynamic vars
   (e.g. test-isolation `*test-conn*`) propagate into the event-loop
   thread when the handler runs.

   On JS: Uses setTimeout for async behavior."
  [event]
  #?(:clj (do
            (start-event-loop!)
            (let [bindings (get-thread-bindings)]
              (async/put! @event-queue {::event event ::bindings bindings})))
     :cljs (js/setTimeout #(process-event event) 0)))

;; =============================================================================
;; Async Extensions (JVM only)

#?(:clj
   (defn dispatch-async
     "Dispatch event and return a channel with the result.

      Useful for waiting on event processing completion. Captures the
      caller's thread bindings so dynamic-var isolation hooks propagate
      into the go-block that runs `process-event`."
     [event]
     (let [result-chan (chan 1)
           bindings    (get-thread-bindings)]
       (go
         (let [context (with-bindings* (or bindings {})
                         (fn [] (process-event event)))]
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
      waits (bounded) for the loop to actually exit, then installs a fresh
      channel and resets the processing flag.

      After calling stop!, you can restart by calling init! and dispatching events."
     []
     (let [was-running? (compare-and-set! processing? true false)
           exited       @loop-exit]
       (when was-running?
         (async/close! @event-queue))
       ;; Wait for the previous go-loop to actually terminate before
       ;; swapping in a new channel, so a rapid stop/start cycle can't
       ;; leak the old consumer onto the new queue. Bounded to 1s so a
       ;; stuck loop can't deadlock the caller.
       (when (and was-running? exited)
         (deref exited 1000 :timeout))
       (reset! loop-exit nil)
       (reset! event-queue (chan 1024)))))
