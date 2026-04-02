(ns hive.events.cofx
  "Coeffects (cofx) system - dependency injection for event handlers.

   Ported from re-frame/cofx.cljc with JVM compatibility.

   Coeffects are input data that handlers need but shouldn't fetch themselves.
   They enable pure handlers by injecting all dependencies.

   Built-in coeffects:
   - :db    - Current application state
   - :event - The event being processed

   Usage:
     ;; Register a coeffect handler
     (reg-cofx :now
       (fn [coeffects]
         (assoc coeffects :now (System/currentTimeMillis))))

     ;; Inject into event handler
     (reg-event-fx :log/timestamp
       [(inject-cofx :now)]
       (fn [{:keys [db now]} _]
         {:db (assoc db :last-timestamp now)}))"
  (:require [hive.events.interceptor :as interceptor]
            [hive.events.log :as log]))

(defonce ^:private cofx-registry (atom {}))

(defn reg-cofx
  "Register a coeffect handler.

   (reg-cofx :cofx-id
     (fn [coeffects]
       (assoc coeffects :cofx-id (compute-value))))

   Coeffect handlers:
   - Receive the current coeffects map
   - Return updated coeffects map with new key(s) added
   - Should be relatively fast (called during event processing)"
  [id handler]
  (when-let [existing (get @cofx-registry id)]
    (log/warn "overwriting cofx handler" id))
  (swap! cofx-registry assoc id handler))

(defn clear-cofx
  "Clear coeffect handler. If no id provided, clear all."
  ([]
   (reset! cofx-registry {}))
  ([id]
   (swap! cofx-registry dissoc id)))

(defn unreg-cofx
  "Remove coeffect handler for cofx-id.
   Returns true if the handler was found and removed, false if not found.
   Thread-safe (uses swap! on atom)."
  [id]
  (let [removed? (atom false)]
    (swap! cofx-registry
           (fn [registry]
             (if (contains? registry id)
               (do (reset! removed? true)
                   (dissoc registry id))
               registry)))
    @removed?))

(defn get-cofx
  "Get coeffect handler by id."
  [id]
  (get @cofx-registry id))

(defn registered-cofx-ids
  "Return set of registered coeffect handler IDs."
  []
  (set (keys @cofx-registry)))

(defn inject-cofx
  "Create an interceptor that injects a coeffect.

   Can be used with or without a value parameter:
     (inject-cofx :now)           ; calls handler with no extra arg
     (inject-cofx :config :prod)  ; calls handler with :prod as second arg

   The coeffect handler receives [coeffects] or [coeffects value]."
  ([id]
   (inject-cofx id nil))
  ([id value]
   (interceptor/->interceptor
    :id (keyword (str "inject-cofx-" (name id)))
    :before (fn [context]
              (if-let [handler (get-cofx id)]
                (update context :coeffects
                        (fn [cofx]
                          (if (some? value)
                            (handler cofx value)
                            (handler cofx))))
                (do
                  (log/warn "no cofx handler for" id)
                  context))))))

;; =============================================================================
;; Built-in Coeffects

;; :db is injected by the router, not here, since it needs access to app-db atom

;; :event is automatically available in coeffects

(reg-cofx :now
          (fn [coeffects]
            (assoc coeffects :now #?(:clj (System/currentTimeMillis)
                                     :cljs (.getTime (js/Date.))))))

(reg-cofx :random
          (fn [coeffects]
            (assoc coeffects :random (rand))))

(reg-cofx :uuid
          (fn [coeffects]
            (assoc coeffects :uuid #?(:clj (str (java.util.UUID/randomUUID))
                                      :cljs (str (random-uuid))))))
