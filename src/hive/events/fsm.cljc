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

   Based on yogthos/maestro v0.2.1 (MIT License)."
  (:refer-clojure :exclude [compile])
  #?(:clj (:require [sci.core :as sci]
                    [hive.events.fx :as fx])
     :cljs (:require [sci.core :as sci]
                     [hive.events.fx :as fx])))

;; =============================================================================
;; Defaults
;; =============================================================================

(defn default-on-end
  "Returns the :data key from the FSM state map."
  [_resources {:keys [data]}]
  data)

(defn default-on-error
  "Default error handler — throws ex-info with FSM state as data."
  [_resources fsm]
  (throw (ex-info "FSM execution error" (select-keys fsm [:data :error :trace :current-state-id]))))

;; =============================================================================
;; Handler Normalization
;; =============================================================================

(defn normalize-handler
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

(defn validate-state-spec
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

(defn add-trace-segment
  "Append a trace segment, keeping at most `max-trace` entries."
  [trace max-trace segment]
  (vec (take-last max-trace (conj trace segment))))

;; =============================================================================
;; Subscriptions
;; =============================================================================

(defn run-subscriptions
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
      :or   {max-trace 1000
             pre       (fn [fsm _resources] fsm)
             post      (fn [fsm _resources] fsm)}} :opts}
    resources
    {init-trace    :trace
     init-data     :data
     init-state-id :current-state-id
     :or           {init-state-id ::start
                    init-trace    []
                    init-data     {}}}]
   (let [;; Initialize FSM state
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
