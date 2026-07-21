(ns hive.events.observer
  "Observer seam for dispatched events.

   The event registry holds ONE handler per event id. Observers are the
   extension point for cross-cutting side effects that must not own, replace
   or know about that handler.

   Extend by implementing IEventObserver — `EventIdObserver` (a fixed set of
   event ids) and `GlobalObserver` (every event) are supplied; a consumer may
   register any other implementation.

   Contract: observers run AFTER the handler's effects have been applied, in
   observer-id order, and each is isolated — an observer that throws is logged
   and never propagates to the handler or to sibling observers."
  (:require [hive.events.log :as log]))

(defprotocol IEventObserver
  (observer-id [this]
    "Unique id for this observer. Registering the same id twice replaces it.")
  (observes? [this event-id]
    "True when this observer should be notified of `event-id`.")
  (on-event [this context]
    "Side effect for an observed event. Return value is ignored."))

(defrecord EventIdObserver [id event-ids f]
  IEventObserver
  (observer-id [_] id)
  (observes? [_ event-id] (contains? event-ids event-id))
  (on-event [_ context] (f context)))

(defrecord GlobalObserver [id f]
  IEventObserver
  (observer-id [_] id)
  (observes? [_ _event-id] true)
  (on-event [_ context] (f context)))

(defn on-events
  "Observer of `event-ids` only. `f` is (fn [context] ...)."
  [id event-ids f]
  (->EventIdObserver id (set event-ids) f))

(defn on-any-event
  "Observer of every dispatched event. `f` is (fn [context] ...)."
  [id f]
  (->GlobalObserver id f))

(defonce ^:private registry (atom {}))

(defn register-observer!
  "Register `observer`, replacing any observer with the same id.
   Returns its id."
  [observer]
  (let [id (observer-id observer)]
    (swap! registry assoc id observer)
    id))

(defn remove-observer!
  "Deregister the observer registered under `id`. Returns nil."
  [id]
  (swap! registry dissoc id)
  nil)

(defn observers
  "Map of registered {observer-id observer}."
  []
  @registry)

(defn clear-observers!
  "Deregister every observer. Returns nil."
  []
  (reset! registry {})
  nil)

(defn select-observers
  "Observers in `observer-map` that observe `event-id`, ordered by observer-id.
   Pure."
  [observer-map event-id]
  (->> (vals observer-map)
       (filter #(observes? % event-id))
       (sort-by (comp str observer-id))
       vec))

(defn notify!
  "Notify every registered observer of `event-id`, passing `context`.
   Each observer is isolated: a throw is logged and swallowed.
   Returns the number of observers notified."
  [event-id context]
  (let [selected (select-observers @registry event-id)]
    (doseq [o selected]
      (try
        (on-event o context)
        (catch #?(:clj Throwable :cljs :default) t
          (log/warn "event observer failed" (observer-id o) event-id t))))
    (count selected)))
