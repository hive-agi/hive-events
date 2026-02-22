(ns hive.events.fx
  "Effects (fx) system - side effect handlers.

   Ported from re-frame/fx.cljc with JVM compatibility.

   Effects are declarative descriptions of side effects.
   Each effect type has a registered handler that performs the actual work.

   Built-in effects:
   - :db         - Update application state
   - :dispatch   - Dispatch another event
   - :dispatch-n - Dispatch multiple events

   Usage:
     ;; Register a custom effect handler
     (reg-fx :http
       (fn [{:keys [method url on-success]}]
         (http-request method url
           (fn [response]
             (dispatch [on-success response])))))

     ;; Event handler returns effects map
     (reg-event-fx :user/login
       (fn [{:keys [db]} [_ credentials]]
         {:db (assoc db :loading? true)
          :http {:method :post :url \"/api/login\" :body credentials}}))"
  (:require [hive.events.log :as log]))

(defonce ^:private fx-registry (atom {}))

(def ^:dynamic *fx-interceptor*
  "Dynamic var for intercepting effect execution.
   When bound to a function, `do-fx-seq` calls this instead of executing effects directly.
   Used by `run-sub-fsm-fx` to capture child effects without executing them (thread-safe).
   Default: nil (effects execute normally)."
  nil)

(defn reg-fx
  "Register an effect handler.

   (reg-fx :effect-id
     (fn [effect-value]
       ;; perform side effect
       ))

   Effect handlers:
   - Receive the effect value from the effects map
   - Perform the side effect
   - Return value is ignored"
  [id handler]
  (when-let [existing (get @fx-registry id)]
    (log/warn "overwriting fx handler" id))
  (swap! fx-registry assoc id handler))

(defn clear-fx
  "Clear effect handler. If no id provided, clear all."
  ([]
   (reset! fx-registry {}))
  ([id]
   (swap! fx-registry dissoc id)))

(defn get-fx
  "Get effect handler by id."
  [id]
  (get @fx-registry id))

(defn- invoke-fx-handler
  "Look up and invoke a registered fx handler for a single effect.
   Logs warning if handler not found, catches and logs exceptions.
   Shared by both `do-fx` (map) and `do-fx-seq` (sequential)."
  [effect-id effect-value]
  (if-let [handler (get-fx effect-id)]
    (try
      (handler effect-value)
      (catch #?(:clj Exception :cljs :default) e
        (log/error "error in fx handler" effect-id e)))
    (log/warn "no fx handler for" effect-id)))

(defn do-fx
  "Execute all effects in an effects map.

   Processes effects in undefined order (except :db which is always first).
   Unknown effects are warned about but don't throw."
  [effects]
  (when (map? effects)
    ;; Process :db first if present
    (when-let [db-effect (get effects :db)]
      (when-let [handler (get-fx :db)]
        (handler db-effect)))

    ;; Process remaining effects
    (doseq [[effect-id effect-value] (dissoc effects :db)]
      (invoke-fx-handler effect-id effect-value))))

(defn do-fx-seq
  "Execute effects from a sequential collection of [effect-id value] tuples.

   Unlike `do-fx` (which takes a map), this preserves ordering and allows
   the same effect-id to appear multiple times.

   Used by the FSM engine when handlers return `{:data ... :fx [...]}`.

   When `*fx-interceptor*` is bound, delegates to it instead of executing
   effects directly. This enables sub-FSM effect capture (thread-safe).

   Example:
     (do-fx-seq [[:log {:msg \"starting\"}]
                  [:http {:url \"/api\"}]
                  [:log {:msg \"done\"}]])"
  [effects]
  (when (sequential? effects)
    (if *fx-interceptor*
      (*fx-interceptor* effects)
      (doseq [[effect-id effect-value] effects]
        (invoke-fx-handler effect-id effect-value)))))

;; =============================================================================
;; Built-in Effects

(reg-fx :dispatch
        (fn [event]
    ;; Circular dependency - router will inject this
          (when-let [dispatch-fn (get @fx-registry :dispatch-impl)]
            (dispatch-fn event))))

(reg-fx :dispatch-n
        (fn [events]
          (when-let [dispatch-fn (get @fx-registry :dispatch-impl)]
            (doseq [event events]
              (dispatch-fn event)))))

(reg-fx :dispatch-later
        (fn [dispatches]
          (when-let [dispatch-fn (get @fx-registry :dispatch-impl)]
            (doseq [{:keys [ms dispatch]} dispatches]
              #?(:clj (future
                        (Thread/sleep ms)
                        (dispatch-fn dispatch))
                 :cljs (js/setTimeout #(dispatch-fn dispatch) ms))))))
