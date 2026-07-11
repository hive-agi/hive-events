(ns hive.events.fsm
  "Deterministic FSM workflow engine for hive-events.

   Adapted from yogthos/maestro — state machine runner for expressing
   workflows as data. Integrated with hive-events fx/cofx system.

   ## Usage

   ```clojure
   (require '[hive.events.fsm :as fsm])

   ;; Define FSM spec
   (def my-workflow
     {:fsm {::fsm/start {:handler    (fn [resources data]
                                       (assoc data :step :ready))
                          :dispatches [[::process (constantly true)]]}
            ::process    {:handler    (fn [resources data]
                                       (assoc data :result ((:compute resources) data)))
                          :dispatches [[::fsm/end (fn [{:keys [result]}] (some? result))]
                                       [::fsm/error (constantly true)]]}}
      :opts {:max-trace 100}})

   ;; Compile and run
   (-> (fsm/compile my-workflow)
       (fsm/run {:compute my-fn} {:data {:input 42}}))
   ```

   ## Privileged States
   - `::start` — initial state
   - `::end`   — terminal success, returns data
   - `::halt`  — pausable, returns FSM state for resume
   - `::error` — terminal failure

   ## Integration with hive-events
   - Handlers are pure: `(fn [resources data] new-data)`
   - Handlers may return fx-enhanced results:
     `(fn [resources data] {:data new-data, :fx [[:effect-id params]]})`
     Effects are processed through the hive.events.fx pipeline after transition.
   - Subscriptions can dispatch events on state path changes
   - Pre/post hooks for interceptor-style concerns
   - EDN specs compiled with SCI
   - Sub-FSM composition via `run-sub-fsm` and `make-sub-fsm-handler`

   Based on yogthos/maestro v0.2.1 (MIT License)."
  (:refer-clojure :exclude [compile])
  #?(:clj (:require [sci.core :as sci]
                    [hive.events.fx :as fx])
     :cljs (:require [sci.core :as sci]
                     [hive.events.fx :as fx])))

;; =============================================================================
;; Defaults
;; =============================================================================

(defn- default-on-end
  "Returns the :data key from the FSM state map."
  [_resources {:keys [data]}]
  data)

(defn- default-on-error
  "Default error handler — throws ex-info with FSM state as data."
  [_resources fsm]
  (throw (ex-info "FSM execution error" (select-keys fsm [:data :error :trace :current-state-id]))))

;; =============================================================================
;; Handler Normalization
;; =============================================================================

(defn- normalize-handler
  "Wrap sync handlers to match the async callback interface.
   Async handlers (`:async? true`) are passed through unchanged.

   Normalized signature: (fn [resources data callback error-callback])"
  [state-id handler async?]
  (if async?
    handler
    (fn [resources data callback error-callback]
      (try
        (callback (handler resources data))
        (catch #?(:clj Exception :cljs :default) ex
          (error-callback
           (ex-info "handler error"
                    {:current-state-id state-id
                     :data             data
                     :error            ex})))))))

;; =============================================================================
;; Spec Validation & Compilation
;; =============================================================================

(defn- validate-state-spec
  "Validate a single state spec. Handlers are required for all states.
   Dispatches are required for non-terminal states."
  [id {:keys [handler dispatches] :as spec}]
  (when (nil? handler)
    (throw (ex-info (str "missing handler for state " id) {:id id :spec spec})))
  (when (and (not (contains? #{::end ::halt ::error} id))
             (nil? dispatches))
    (throw (ex-info (str "missing dispatches for state " id) {:id id :spec spec}))))

(defn- compile-state-handler
  "Compile a single state: normalize handler, eval dispatch predicates via SCI."
  [state-id {:keys [handler dispatches async?]} sci-ctx valid-targets]
  {:handler    (normalize-handler state-id handler async?)
   :dispatches (for [[target pred] dispatches]
                 (if-not (contains? valid-targets target)
                   (throw (ex-info (str "invalid dispatch target " target " from state " state-id)
                                   {:id state-id :target target}))
                   [target (if (fn? pred)
                             pred
                             (sci/eval-form sci-ctx pred))]))})

(defn- compile-dispatches
  "Compile all dispatch predicates in the FSM spec."
  [spec]
  (let [sci-ctx       (sci/init {})
        valid-targets (-> spec :fsm keys set (conj ::end ::halt ::error))]
    (update spec :fsm
            (fn [fsm]
              (reduce-kv
               (fn [acc id state-spec]
                 (validate-state-spec id state-spec)
                 (assoc acc id (compile-state-handler id state-spec sci-ctx valid-targets)))
               {}
               fsm)))))

;; =============================================================================
;; Compile
;; =============================================================================

(defn compile
  "Compile an FSM spec into a runnable state machine.

   Two arities:
   - `(compile spec)` — handlers are inline functions
   - `(compile spec handlers-map)` — handlers are keyword references resolved
     from the map (for EDN specs)

   The compiled FSM should be passed to `run`."
  ([spec handlers-map]
   (compile
    (update spec :fsm
            (fn [fsm]
              (reduce-kv
               (fn [acc k {:keys [handler] :as v}]
                 (let [handler-fn (get handlers-map handler)]
                   (when-not handler-fn
                     (throw (ex-info (str "no handler found for state " k ", handler id " handler)
                                     {:state k :handler-id handler :available (keys handlers-map)})))
                   (assoc acc k (assoc v :handler handler-fn))))
               {}
               fsm)))))
  ([spec]
   (let [end-handler   (get-in spec [:fsm ::end :handler] default-on-end)
         error-handler (get-in spec [:fsm ::error :handler] default-on-error)]
     (-> spec
         (update :fsm dissoc ::end ::error)
         (compile-dispatches)
         (update :fsm merge
                 {::end   {:handler end-handler}
                  ::halt  {:handler (fn [_resources fsm] (dissoc fsm :fsm))}
                  ::error {:handler error-handler}})))))

;; =============================================================================
;; Trace
;; =============================================================================

(defn- add-trace-segment
  "Append a trace segment, keeping at most `max-trace` entries."
  [trace max-trace segment]
  (vec (take-last max-trace (conj trace segment))))

;; =============================================================================
;; Subscriptions
;; =============================================================================

(defn- run-subscriptions
  "Check watched paths in data for changes, call handlers on change.

   Each subscription: `{path {:value old-val :handler (fn [path old new])}}`

   Returns updated subscriptions map with new values.
   Pure — handlers are called for side effects but subscription map
   is returned as new value."
  [data subscriptions]
  (when subscriptions
    (reduce-kv
     (fn [acc path {:keys [value handler] :as sub}]
       (let [new-value (get-in data path)]
         (if (= new-value value)
           (assoc acc path sub)
           (do
             (handler path value new-value)
             (assoc acc path (assoc sub :value new-value))))))
     {}
     subscriptions)))

;; =============================================================================
;; State Transition
;; =============================================================================

(defn- resolve-transition
  "Find the first dispatch target whose predicate returns truthy for data.
   Returns the target state keyword, or nil if no predicate matched."
  [dispatches data]
  (ffirst (drop-while (fn [[_target pred]] (not (pred data))) dispatches)))

(defn- transition
  "Compute the next FSM state after a handler produces new data.
   Validates the target exists in the FSM, appends trace, runs post hook."
  [{:keys [current-state-id opts] :as fsm} dispatches data post resources]
  (let [target-id (resolve-transition dispatches data)]
    (if (and target-id (get-in fsm [:fsm target-id]))
      (-> fsm
          (update :trace add-trace-segment (:max-trace opts)
                  {:state-id current-state-id :status :success})
          (assoc :current-state-id target-id
                 :last-state-id current-state-id
                 :data data)
          (post resources))
      (throw (ex-info "no valid state transition"
                      {:current-state-id current-state-id
                       :target-state-id  target-id
                       :data             data})))))

;; =============================================================================
;; FX Integration
;; =============================================================================

(defn- fx-result?
  "Check if a handler result is an fx-enhanced result map.
   Returns true if result is a map with a sequential :fx key.

   Handlers can return either:
   - Plain data (backward compatible): `{:count 1, :status :ok}`
   - FX-enhanced result: `{:data {:count 1}, :fx [[:log {:msg \"done\"}]]}`"
  [result]
  (and (map? result)
       (contains? result :fx)
       (sequential? (:fx result))))

(defn- extract-handler-result
  "Extract data and effects from a handler result.

   If fx-enhanced: returns [data effects-vec]
   If plain data:  returns [data nil]"
  [result]
  (if (fx-result? result)
    [(:data result) (:fx result)]
    [result nil]))

;; =============================================================================
;; Transition tap (opt-in observer)
;; =============================================================================

(defonce ^:private run-seq (atom 0))

(defn- next-run-token []
  (str "fsmrun-" (swap! run-seq inc)))

(defonce ^:private transition-tap (atom nil))

(defn set-transition-tap!
  "Register a per-transition observer (fn [run-token fsm]); nil clears it."
  [f]
  (reset! transition-tap f)
  :ok)

(defn- tap! [run-token fsm]
  (when-let [f @transition-tap]
    (try (f run-token fsm) (catch #?(:clj Throwable :cljs :default) _ nil))))

;; =============================================================================
;; Run
;; =============================================================================

(defn run
  "Execute a compiled FSM.

   Three arities:
   - `(run fsm)` — empty resources and state
   - `(run fsm resources)` — empty initial state
   - `(run fsm resources initial-state)` — full control

   Resources: map of external dependencies (DB conns, config, etc.)
   Initial state keys:
   - `:data`             — initial data map
   - `:current-state-id` — resume from specific state
   - `:trace`            — existing trace log

   Returns the final data map (on ::end) or halted FSM (on ::halt)."
  ([compiled-fsm]
   (run compiled-fsm {} {}))
  ([compiled-fsm resources]
   (run compiled-fsm resources {}))
  ([{fsm-graph :fsm
     {:keys [max-trace subscriptions pre post]
      opts-run-id :run-id
      :or   {max-trace 1000
             pre       (fn [fsm _resources] fsm)
             post      (fn [fsm _resources] fsm)}} :opts}
    resources
    {init-trace    :trace
     init-data     :data
     init-state-id :current-state-id
     init-run-id   :run-id
     :or           {init-state-id ::start
                    init-trace    []
                    init-data     {}}}]
   (let [run-token (or init-run-id opts-run-id (next-run-token))
         ;; Initialize FSM state
         init-fsm (post
                   {:fsm              fsm-graph
                    :current-state-id init-state-id
                    :last-state-id    (:state-id (last init-trace))
                    :data             init-data
                    :trace            init-trace
                    :opts             {:max-trace     max-trace
                                       :subscriptions (reduce-kv
                                                       (fn [m path sub]
                                                         (assoc m path
                                                                (assoc sub :value (get-in init-data path))))
                                                       {}
                                                       (or subscriptions {}))}}
                   resources)]
     (loop [{:keys [data current-state-id last-state-id opts] :as fsm}
            (pre init-fsm resources)]
       (let [{:keys [handler dispatches]} (get-in fsm [:fsm current-state-id])
             ;; Run subscriptions on current data
             fsm (assoc-in fsm [:opts :subscriptions]
                           (run-subscriptions data (:subscriptions opts)))]
         (tap! run-token fsm)
         (cond
           ;; Terminal states — invoke handler and return
           (= ::end current-state-id)
           (handler resources fsm)

           (= ::halt current-state-id)
           (handler resources (assoc fsm :current-state-id last-state-id
                                     :last-state-id nil))

           (= ::error current-state-id)
           (handler resources fsm)

           ;; Normal state — run handler, transition, recur
           :else
           (let [result   (try
                            (handler resources data
                                     identity  ;; callback (sync path)
                                     (fn [err] (throw err)))
                            (catch #?(:clj Exception :cljs :default) ex
                              ;; Transition to error state
                              (throw (ex-info "handler error"
                                              {:current-state-id current-state-id
                                               :data             data
                                               :error            ex}))))
                 ;; Support fx-enhanced results: {:data new-data, :fx [[:effect-id params]]}
                 [new-data effects] (extract-handler-result result)
                 next-fsm (try
                            (transition fsm dispatches new-data post resources)
                            (catch #?(:clj Exception :cljs :default) ex
                              ;; No valid transition — move to error
                              (-> fsm
                                  (update :trace add-trace-segment max-trace
                                          {:state-id current-state-id :status :error})
                                  (assoc :current-state-id ::error
                                         :error ex))))]
             ;; Process effects after successful state transition
             (when effects
               (fx/do-fx-seq effects))
             (recur (pre next-fsm resources)))))))))

;; =============================================================================
;; Convenience: run-async (JVM only)
;; =============================================================================

#?(:clj
   (defn run-async
     "Execute FSM on a separate thread, return a promise.

      Useful for long-running workflows that shouldn't block the caller.

      Returns a future that yields the FSM result."
     ([compiled-fsm]
      (run-async compiled-fsm {} {}))
     ([compiled-fsm resources]
      (run-async compiled-fsm resources {}))
     ([compiled-fsm resources initial-state]
      (future (run compiled-fsm resources initial-state)))))

;; =============================================================================
;; Convenience: step (for debugging/testing)
;; =============================================================================

(defn step
  "Execute a single state transition (for debugging/testing).

   Takes a running FSM state (as returned by ::halt handler) and
   executes one step, returning the new FSM state.

   Useful for step-through debugging of workflows."
  [{:keys [fsm current-state-id data] :as halted-fsm} resources]
  (let [{:keys [handler dispatches]} (get fsm current-state-id)]
    (when-not handler
      (throw (ex-info "no handler for state" {:state current-state-id})))
    (let [result   (handler resources data identity (fn [e] (throw e)))
          [new-data effects] (extract-handler-result result)
          next-fsm (transition (assoc halted-fsm :fsm fsm)
                               dispatches new-data
                               identity resources)]
      (when effects
        (fx/do-fx-seq effects))
      next-fsm)))

;; =============================================================================
;; Sub-FSM Composition
;; =============================================================================

(defn run-sub-fsm
  "Execute a compiled sub-FSM within a parent handler context.

   Runs the child FSM to completion and returns its result data,
   which the parent handler can merge into its own data map.

   Args:
     compiled-child — Compiled FSM (from `compile`)
     resources      — Resources map (shared or child-specific)
     initial-data   — Initial data for the child FSM

   Returns:
     The child FSM result (final data map on ::end).

   Error handling:
     Catches child FSM exceptions and returns an error map:
     `{:sub-fsm/error true :sub-fsm/message str :sub-fsm/cause ex}`
     This lets the parent handler decide how to handle child failures
     without crashing the parent FSM.

   Example:
     ```clojure
     (defn handle-with-context [resources data]
       (let [ctx-result (run-sub-fsm compiled-context-fsm
                                     resources
                                     (select-keys data [:agent-id :directory]))]
         (if (:sub-fsm/error ctx-result)
           (assoc data :error (:sub-fsm/message ctx-result))
           (merge data ctx-result))))
     ```"
  [compiled-child resources initial-data]
  (try
    (run compiled-child resources {:data initial-data})
    (catch #?(:clj Exception :cljs :default) ex
      {:sub-fsm/error   true
       :sub-fsm/message (#?(:clj ex-message :cljs .-message) ex)
       :sub-fsm/cause   ex})))

(defn run-sub-fsm-fx
  "Execute a compiled sub-FSM and return both data and accumulated effects.

   Like `run-sub-fsm` but captures FX from child handlers instead of
   executing them immediately. The parent handler can then include these
   effects in its own `:fx` vector, surfacing them to the parent FSM's
   FX pipeline.

   This is essential for composability — child FSMs should not execute
   side effects independently when embedded in a parent context. The
   parent must control effect execution order.

   Args:
     compiled-child — Compiled FSM (from `compile`)
     resources      — Resources map (shared or child-specific)
     initial-data   — Initial data for the child FSM

   Returns:
     `{:data result-data, :fx [...]}` — child data + accumulated effects.
     On error: `{:sub-fsm/error true, :sub-fsm/message str, :sub-fsm/cause ex}`

   Example:
     ```clojure
     (defn handle-with-effects [resources data]
       (let [{child-data :data child-fx :fx :as result}
             (run-sub-fsm-fx compiled-child resources
                             (select-keys data [:input]))]
         (if (sub-fsm-error? result)
           (assoc data :error (:sub-fsm/message result))
           {:data (merge data child-data)
            :fx (vec child-fx)})))
     ```"
  [compiled-child resources initial-data]
  (let [fx-acc (atom [])]
    (try
      (binding [fx/*fx-interceptor* (fn [effects]
                                      (when (sequential? effects)
                                        (swap! fx-acc into effects)))]
        (let [result (run compiled-child resources {:data initial-data})]
          {:data result
           :fx   (vec @fx-acc)}))
      (catch #?(:clj Exception :cljs :default) ex
        {:sub-fsm/error   true
         :sub-fsm/message (#?(:clj ex-message :cljs .-message) ex)
         :sub-fsm/cause   ex}))))

(defn sub-fsm-error?
  "Check if a sub-FSM result is an error map (from run-sub-fsm)."
  [result]
  (true? (:sub-fsm/error result)))

(defn make-sub-fsm-handler
  "Create a parent FSM handler that delegates to a compiled sub-FSM.

   The returned handler:
   1. Extracts child initial data from parent data via `data-fn`
   2. Runs the child FSM via `run-sub-fsm`
   3. Merges child result into parent data via `merge-fn`

   Args:
     compiled-child — Compiled child FSM
     opts           — Options map:
       :data-fn     — (fn [parent-data] child-initial-data)
                      Default: identity (pass full parent data)
       :merge-fn    — (fn [parent-data child-result] merged-data)
                      Default: merge (shallow merge child into parent)
       :result-key  — If provided, assoc child result under this key
                      instead of using merge-fn (convenience shorthand)
       :resources-fn — (fn [parent-resources] child-resources)
                       Default: identity (share parent resources)
       :error-key   — Key to store error message when sub-FSM fails
                      Default: :error

   Returns:
     (fn [resources data] data') — a standard FSM handler.

   Example:
     ```clojure
     ;; Delegate to context-gather sub-FSM, store under :context
     (def my-spec
       {:fsm {::fsm/start
              {:handler (make-sub-fsm-handler
                          compiled-context-gather
                          {:data-fn    #(select-keys % [:agent-id :directory])
                           :result-key :context})
               :dispatches [[::next (constantly true)]]}}})
     ```"
  ([compiled-child]
   (make-sub-fsm-handler compiled-child {}))
  ([compiled-child {:keys [data-fn merge-fn result-key resources-fn error-key fx?]
                    :or   {data-fn      identity
                           merge-fn     merge
                           resources-fn identity
                           error-key    :error
                           fx?          false}}]
   (fn [resources data]
     (let [child-data      (data-fn data)
           child-resources (resources-fn resources)
           result          (if fx?
                             (run-sub-fsm-fx compiled-child child-resources child-data)
                             (run-sub-fsm compiled-child child-resources child-data))]
       (if (sub-fsm-error? result)
         (assoc data error-key (:sub-fsm/message result))
         (if fx?
           ;; FX mode: return {:data ..., :fx [...]} for parent FSM pipeline
           (let [child-result (:data result)
                 child-fx     (:fx result)
                 merged       (if result-key
                                (assoc data result-key child-result)
                                (merge-fn data child-result))]
             (if (seq child-fx)
               {:data merged :fx child-fx}
               merged))
           ;; Non-FX mode: plain data merge
           (if result-key
             (assoc data result-key result)
             (merge-fn data result))))))))
