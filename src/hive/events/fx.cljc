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

(defn unreg-fx
  "Remove effect handler for fx-id.
   Returns true if the handler was found and removed, false if not found.
   Thread-safe (uses swap! on atom)."
  [id]
  (let [removed? (atom false)]
    (swap! fx-registry
           (fn [registry]
             (if (contains? registry id)
               (do (reset! removed? true)
                   (dissoc registry id))
               registry)))
    @removed?))

(defn get-fx
  "Get effect handler by id."
  [id]
  (get @fx-registry id))

(defn registered-fx-ids
  "Return set of registered effect handler IDs."
  []
  (set (keys @fx-registry)))

(defn registry-snapshot
  "Current effect registry value. For inspection and save/restore in tests."
  []
  @fx-registry)

(defn restore-registry!
  "Replace the whole effect registry with `handlers`. For test isolation."
  [handlers]
  (reset! fx-registry handlers))

(defn default-invoke-fx-handler
  "Look up and invoke a registered fx handler for a single effect.
   Logs a warning if no handler is registered, catches and logs exceptions.
   Never throws.

   This is the DEFAULT policy. `set-fx-executor!` replaces it."
  [effect-id effect-value]
  (if-let [handler (get-fx effect-id)]
    (try
      (handler effect-value)
      (catch #?(:clj Exception :cljs :default) e
        (log/error "error in fx handler" effect-id e)))
    (log/warn "no fx handler for" effect-id)))

(defonce ^:private fx-executor
  ^{:doc "The installed effect-invocation policy, or nil for the default."}
  (atom nil))

(defn set-fx-executor!
  "Install `f` as the policy that invokes ONE effect: (f effect-id effect-value).

   The policy owns handler lookup and error handling, so an embedder can add
   metrics, tracing or a loud-fail counter for unregistered effects without the
   library knowing what any of those are. `default-invoke-fx-handler` is the
   behaviour to fall back on, and a policy that wants only to observe should
   call it.

   A policy MUST NOT throw: `do-fx` runs after the interceptor chain has
   already committed, so a throw here surfaces as a dispatch failure for work
   that already happened.

   Returns `f`. One executor at a time, last writer wins."
  [f]
  (reset! fx-executor f)
  f)

(defn clear-fx-executor!
  "Restore the default effect-invocation policy. Returns nil."
  []
  (reset! fx-executor nil))

(defn fx-executor-installed?
  "True when a non-default effect-invocation policy is installed."
  []
  (some? @fx-executor))

(defn- invoke-fx-handler
  "Invoke one effect through the installed policy, or the default.
   Shared by `do-fx` (map) and `do-fx-seq` (sequential)."
  [effect-id effect-value]
  ((or @fx-executor default-invoke-fx-handler) effect-id effect-value))

(defn do-fx
  "Execute all effects in an effects map.

   `:db` runs first when present; the rest run in undefined order. Unknown
   effects are warned about and do not throw.

   Every effect, `:db` included, goes through the installed fx executor
   (`set-fx-executor!`), so an embedder's metrics and unregistered-effect
   counters see the whole set rather than all of it except `:db`."
  [effects]
  (when (map? effects)
    (when (contains? effects :db)
      (invoke-fx-handler :db (get effects :db)))
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
