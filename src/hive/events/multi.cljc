(ns hive.events.multi
  "Multimethod-based event dispatch + pure orchestration for hive-events.

   Two complementary subsystems in one namespace:

   ## 1. Handler Dispatch (multimethod-based)

   Registration:
   - `(register-handler! event-id handler-fn)` — register handler (no interceptors)
   - `(register-handler! event-id interceptors handler-fn)` — with interceptors
   - `(remove-handler! event-id)` — remove a registered handler

   Dispatch:
   - `(dispatch-sync event)` — synchronous dispatch + fx execution
   - `(event-id event)` — extract event-id (dispatch fn)

   Query:
   - `(handler-registered? event-id)` — check if handler exists

   Handler signature: `(fn [coeffects event] effects-map)`

   ## 2. Pure Orchestration (multi-op compilation)

   Compile a batch of cross-tool operations into a wave-based execution plan.
   Pure functions — no side effects, no IO, no requiring-resolve.

   Pipeline: normalize → validate → expand-batch → assign-waves → compile-spec

   - `(normalize-op op)` — normalize string keys, auto-generate ID
   - `(validate-ops ops)` — validate IDs, deps, cycles (Kahn's algorithm)
   - `(expand-batch-macro ops)` — expand 'batch' command into fan-out sub-ops
   - `(assign-waves ops)` — topological sort into dependency-ordered wave groups
   - `(compile-multi-spec ops)` — end-to-end: validate → expand → waves → FSM spec

   ## Architecture

   ```
   hive-events (this ns)  — pure compilation (validate, sort, waves)
     ↓
   hive-mcp tools/multi   — bridge (resolve handlers, execute ops, format)
     ↓
   hive-mcp consolidated  — MCP facade (JSON schema, tool def)
   ```

   SOLID: OCP — Open for extension via defmethod handlers
   SOLID: SRP — Pure orchestration logic, no side effects
   CLARITY: L — Layered: pure compilation separate from execution"
  (:require [hive.events.interceptor :as interceptor]
            [hive.events.fx :as fx]
            [clojure.string :as str]))

;; =============================================================================
;; Handler Registry (multimethod-backed)
;; =============================================================================

(defn event-id
  "Extract the event-id (first element) from an event vector.
   This is the dispatch function for the handle multimethod."
  [event]
  (first event))

(defmulti handle
  "Multimethod for event handling. Dispatches on (first event).

   Methods are installed via `register-handler!`, not directly via `defmethod`.
   This allows attaching interceptors alongside the handler function.

   Each method returns: {:handler fn, :interceptors [...]}
   where handler is (fn [coeffects event] effects-map)."
  event-id)

(defmethod handle :default [event]
  (throw (ex-info (str "No handler registered for event: " (first event))
                  {:event event
                   :event-id (first event)})))

;; =============================================================================
;; Handler Metadata Registry
;; =============================================================================

;; Stores interceptors per event-id (multimethods can't carry metadata)
(defonce ^:private handler-meta (atom {}))

(defn register-handler!
  "Register an event handler with optional interceptors.

   Two arities:
   - (register-handler! event-id handler-fn)
   - (register-handler! event-id interceptors handler-fn)

   Handler signature: (fn [coeffects event] effects-map)
   Interceptors: vector of interceptor maps (from ->interceptor)"
  ([event-id handler-fn]
   (register-handler! event-id [] handler-fn))
  ([event-id interceptors handler-fn]
   ;; Remove existing method if any (for override support)
   (when (get-method handle event-id)
     (remove-method handle event-id))
   ;; Store interceptors
   (swap! handler-meta assoc event-id {:interceptors (vec interceptors)
                                       :handler handler-fn})
   ;; Install multimethod
   #?(:clj (.addMethod ^clojure.lang.MultiFn handle event-id
                       (fn [event]
                         (get @handler-meta event-id)))
      :cljs (-add-method handle event-id
                         (fn [event]
                           (get @handler-meta event-id))))))

(defn remove-handler!
  "Remove a registered event handler."
  [event-id]
  (remove-method handle event-id)
  (swap! handler-meta dissoc event-id))

(defn handler-registered?
  "Check if a handler is registered for the given event-id."
  [event-id]
  (and (contains? @handler-meta event-id)
       (some? (get-method handle event-id))))

;; =============================================================================
;; Dispatch
;; =============================================================================

(defn dispatch-sync
  "Dispatch an event synchronously through its handler chain.

   1. Looks up handler via multimethod
   2. Builds interceptor chain (registered interceptors + handler interceptor)
   3. Executes chain via hive.events.interceptor/execute
   4. Executes effects via hive.events.fx/do-fx

   Returns the final context with :coeffects and :effects.
   Throws if no handler is registered for the event."
  [event]
  (let [{:keys [interceptors handler]} (handle event)
        ;; Build handler interceptor that converts coeffects -> effects
        handler-interceptor (interceptor/->interceptor
                             :id :handler
                             :before (fn [context]
                                       (let [coeffects (:coeffects context)
                                             event-vec (get coeffects :event)
                                             effects (handler coeffects event-vec)]
                                         (update context :effects merge effects))))
        ;; Full chain: interceptors + handler
        full-chain (conj (vec interceptors) handler-interceptor)
        ;; Execute interceptor chain
        context (interceptor/execute
                 {:coeffects {:event event}
                  :effects {}
                  :queue (vec full-chain)
                  :stack []})]
    ;; Execute side effects
    (fx/do-fx (:effects context))
    context))

;; =============================================================================
;; Pure Orchestration: Operation Normalization
;; =============================================================================

(defn normalize-op
  "Normalize a single operation map for multi-op compilation.

   - Converts string keys to keywords
   - Auto-generates :id if missing
   - Normalizes :depends_on to vector of strings

   Pure function — no side effects.

   Example:
     (normalize-op {\"tool\" \"memory\" \"command\" \"add\" \"id\" \"op-1\"})
     ;; => {:tool \"memory\" :command \"add\" :id \"op-1\" :depends_on []}

     (normalize-op {:tool \"kg\" :command \"edge\"})
     ;; => {:tool \"kg\" :command \"edge\" :id \"op-<uuid>\" :depends_on []}"
  [op]
  (let [normalized (into {} (map (fn [[k v]] [(keyword k) v]) op))]
    (cond-> normalized
      ;; Auto-generate ID if missing
      (str/blank? (str (:id normalized)))
      (assoc :id (str "op-" #?(:clj (java.util.UUID/randomUUID)
                               :cljs (random-uuid))))

      ;; Normalize depends_on to vector of strings
      true
      (update :depends_on (fn [deps]
                            (cond
                              (nil? deps) []
                              (string? deps) [deps]
                              (sequential? deps) (vec deps)
                              :else []))))))

;; =============================================================================
;; Pure Orchestration: Shared Graph Construction + Topological Traversal
;; =============================================================================

(defn- build-graph
  "Build dependency graph from operations.

   Returns {:in-degree {id dep-count}, :fwd {id [dependents]}}.

   - :in-degree maps each op ID to the number of its incoming edges
   - :fwd maps each op ID to the vector of ops that depend on it

   Pure function — shared by validate-ops and assign-waves."
  [ops]
  (let [in-degree (reduce (fn [m {:keys [id depends_on]}]
                            (assoc m id (count (or depends_on []))))
                          {}
                          ops)
        fwd       (reduce (fn [m {:keys [id depends_on]}]
                            (reduce (fn [m' dep]
                                      (update m' dep (fnil conj []) id))
                                    m
                                    (or depends_on [])))
                          {}
                          ops)]
    {:in-degree in-degree :fwd fwd}))

(defn- topo-traverse
  "Generic topological traversal using Kahn's algorithm (wave-level BFS).

   Processes nodes in dependency-ordered waves. Each wave contains all
   nodes whose dependencies have been satisfied.

   Parameters:
   - graph   — {:in-degree {id count}, :fwd {id [dependents]}}
   - step-fn — (fn [acc wave-ids wave-num] new-acc) called once per wave
   - init    — initial accumulator value

   Returns [acc visited-count] where visited-count is total nodes processed.
   If visited-count < total nodes, the graph contains a cycle.

   Pure function — no side effects."
  [{:keys [in-degree fwd]} step-fn init]
  (loop [deg     in-degree
         wave    1
         acc     init
         visited 0]
    (let [ready (into [] (keep (fn [[id d]] (when (zero? d) id))) deg)]
      (if (empty? ready)
        [acc visited]
        (let [acc'  (step-fn acc ready wave)
              deg'  (reduce dissoc deg ready)
              deg'' (reduce (fn [d rid]
                              (reduce (fn [d' child]
                                        (if (contains? d' child)
                                          (update d' child dec)
                                          d'))
                                      d
                                      (get fwd rid [])))
                            deg'
                            ready)]
          (recur deg'' (inc wave) acc' (+ visited (count ready))))))))

;; =============================================================================
;; Pure Orchestration: Validation (Kahn's Algorithm)
;; =============================================================================

(defn validate-ops
  "Validate an operations vector for multi-op execution.

   Returns {:valid true} or {:valid false :errors [...]}.

   Checks:
   - All ops have :id and :tool
   - IDs are unique
   - :depends_on references exist (no dangling refs)
   - No self-dependencies
   - No circular dependencies (Kahn's algorithm)

   Pure function — no side effects, no IO.

   Example:
     (validate-ops [{:id \"op-1\" :tool \"memory\" :command \"add\"}
                     {:id \"op-2\" :tool \"kg\" :command \"edge\" :depends_on [\"op-1\"]}])
     ;; => {:valid true}

     (validate-ops [{:id \"op-1\" :tool \"memory\" :depends_on [\"op-2\"]}
                     {:id \"op-2\" :tool \"kg\" :depends_on [\"op-1\"]}])
     ;; => {:valid false :errors [\"Circular dependency detected among: op-1, op-2\"]}"
  [ops]
  (let [errors    (atom [])
        add-error (fn [msg] (swap! errors conj msg))
        id-set    (set (map :id ops))
        id-counts (frequencies (map :id ops))]

    ;; Check required fields
    (doseq [{:keys [id tool] :as op} ops]
      (when (str/blank? (str id))
        (add-error (str "Operation missing :id — " (pr-str (select-keys op [:tool :command])))))
      (when (str/blank? (str tool))
        (add-error (str "Operation '" id "' missing :tool"))))

    ;; Check duplicate IDs
    (doseq [[id cnt] id-counts]
      (when (> cnt 1)
        (add-error (str "Duplicate operation ID: '" id "' (appears " cnt " times)"))))

    ;; Check dependency references and self-deps
    (doseq [{:keys [id depends_on]} ops]
      (when (seq depends_on)
        (doseq [dep depends_on]
          (when (= dep id)
            (add-error (str "Operation '" id "' depends on itself")))
          (when-not (contains? id-set dep)
            (add-error (str "Operation '" id "' depends on non-existent '" dep "'"))))))

    ;; Check circular dependencies via Kahn's algorithm
    (when (empty? @errors)
      (let [graph (build-graph ops)
            [sorted visited] (topo-traverse graph
                                            (fn [sorted wave-ids _wave-num]
                                              (into sorted wave-ids))
                                            [])]
        (when (< visited (count ops))
          (add-error (str "Circular dependency detected among: "
                          (str/join ", " (remove (set sorted) (map :id ops))))))))

    (if (seq @errors)
      {:valid false :errors @errors}
      {:valid true})))

;; =============================================================================
;; Pure Orchestration: Batch Macro Expansion
;; =============================================================================

(def ^:private known-array-params
  "Parameter keys that are inherently arrays and should NOT be treated
   as fan-out targets during batch expansion."
  #{:depends_on :tags :presets :operations :files :diff_ids
    :task_ids :ids :kg_node_ids :ctx_refs :agents :roles
    :debate_roles :research_roles :participants :options})

(defn expand-batch-macro
  "Expand batch command operations into fan-out sub-operations.

   A batch operation has :command [\"batch\" \"actual-cmd\"].
   Array parameters (excluding known-array-params) are fanned out
   into individual sub-operations. Scalar parameters are broadcast
   to all sub-ops.

   Pure function — no side effects.

   Rules:
   - Non-batch ops pass through unchanged
   - Only ONE array param can be fanned out per batch op (ambiguity error otherwise)
   - Empty fan-out array is an error
   - A join operation is appended that depends on all sub-ops

   Example:
     (expand-batch-macro [{:id \"op-1\" :tool \"memory\" :command [\"batch\" \"add\"]
                            :content [\"note1\" \"note2\"] :type \"note\"}])
     ;; => [{:id \"op-1.0\" :tool \"memory\" :command \"add\" :content \"note1\" :type \"note\"}
     ;;      {:id \"op-1.1\" :tool \"memory\" :command \"add\" :content \"note2\" :type \"note\"}
     ;;      {:id \"op-1\" :tool :noop :command :join :depends_on [\"op-1.0\" \"op-1.1\"]}]"
  [ops]
  (reduce
   (fn [acc {:keys [id tool command depends_on] :as op}]
     (if-not (and (sequential? command)
                  (= "batch" (first command)))
       ;; Non-batch: pass through
       (conj acc op)
       ;; Batch: expand
       (let [actual-cmd (second command)
             ;; Remove meta keys to find candidate fan-out params
             meta-keys #{:id :tool :command :depends_on :wave}
             param-keys (remove #(or (meta-keys %)
                                     (known-array-params %))
                                (keys op))
             ;; Find array-valued params (fan-out candidates)
             array-params (filterv (fn [k]
                                     (let [v (get op k)]
                                       (and (sequential? v)
                                            (not (string? v)))))
                                   param-keys)]
         (cond
           ;; No array params — just unwrap batch command
           (empty? array-params)
           (conj acc (assoc op :command actual-cmd))

           ;; Multiple array params — ambiguous, error
           (> (count array-params) 1)
           (throw (ex-info (str "Batch operation '" id "' has multiple array params: "
                                (str/join ", " (map name array-params))
                                ". Only one fan-out param is supported per batch op.")
                           {:op-id id
                            :array-params array-params}))

           ;; Single array param — fan out
           :else
           (let [fan-key (first array-params)
                 fan-values (get op fan-key)
                 _ (when (empty? fan-values)
                     (throw (ex-info (str "Batch operation '" id "' has empty fan-out array for :" (name fan-key))
                                     {:op-id id :fan-key fan-key})))
                 ;; Scalar params to broadcast
                 scalar-params (dissoc op :id :tool :command :depends_on :wave fan-key)
                 ;; Create sub-ops
                 sub-ops (mapv (fn [idx val]
                                 (merge scalar-params
                                        {:id (str id "." idx)
                                         :tool tool
                                         :command actual-cmd
                                         :depends_on (or depends_on [])
                                         fan-key val}))
                               (range)
                               fan-values)
                 ;; Create join op that depends on all sub-ops
                 join-op {:id id
                          :tool :noop
                          :command :join
                          :depends_on (mapv :id sub-ops)}]
             (into acc (conj sub-ops join-op)))))))
   []
   ops))

;; =============================================================================
;; Pure Orchestration: Wave Assignment (Topological Sort)
;; =============================================================================

(defn assign-waves
  "Assign operations to execution waves based on dependencies.

   Independent ops run in the same wave (parallel). Dependent ops
   are assigned to later waves. Uses Kahn's topological sort.

   Returns ops with :wave key added, sorted by wave number.

   Pure function — no side effects.

   Example:
     ;; Independent ops → same wave
     (assign-waves [{:id \"a\" :depends_on []} {:id \"b\" :depends_on []}])
     ;; => [{:id \"a\" :wave 1 ...} {:id \"b\" :wave 1 ...}]

     ;; Chain → sequential waves
     (assign-waves [{:id \"a\" :depends_on []}
                     {:id \"b\" :depends_on [\"a\"]}])
     ;; => [{:id \"a\" :wave 1 ...} {:id \"b\" :wave 2 ...}]"
  [ops]
  (let [ops-by-id (into {} (map (juxt :id identity) ops))
        graph     (build-graph ops)
        [result _] (topo-traverse graph
                                  (fn [result wave-ids wave-num]
                                    (into result (mapv (fn [id]
                                                         (assoc (get ops-by-id id) :wave wave-num))
                                                       wave-ids)))
                                  [])]
    result))

;; =============================================================================
;; Pure Orchestration: Multi-Spec Compilation
;; =============================================================================

(defn compile-multi-spec
  "Compile a multi-op specification from raw operations.

   End-to-end pipeline: normalize → validate → expand-batch → assign-waves.

   Returns:
   - On success: {:valid true :ops [...] :waves {1 [...] 2 [...]} :wave-count N}
   - On failure: {:valid false :errors [...]}

   The :ops vector contains fully normalized, wave-assigned operations.
   The :waves map groups ops by wave number for execution.

   Pure function — no side effects, no IO.

   Example:
     (compile-multi-spec [{:tool \"memory\" :command \"add\" :content \"hello\" :type \"note\"}
                           {:tool \"kg\" :command \"edge\" :from \"a\" :to \"b\"
                            :depends_on []}])
     ;; => {:valid true
     ;;     :ops [{:id \"op-...\" :tool \"memory\" ... :wave 1}
     ;;            {:id \"op-...\" :tool \"kg\" ... :wave 1}]
     ;;     :waves {1 [{...} {...}]}
     ;;     :wave-count 1}"
  [ops]
  (let [;; Step 1: Normalize
        normalized (mapv normalize-op ops)
        ;; Step 2: Validate
        validation (validate-ops normalized)]
    (if-not (:valid validation)
      {:valid false :errors (:errors validation)}
      ;; Step 3: Expand batch macros
      (try
        (let [expanded (expand-batch-macro normalized)
              ;; Re-validate after expansion (new ops may have been introduced)
              re-validation (validate-ops expanded)]
          (if-not (:valid re-validation)
            {:valid false :errors (:errors re-validation)}
            ;; Step 4: Assign waves
            (let [waved-ops (assign-waves expanded)
                  wave-groups (group-by :wave waved-ops)]
              {:valid true
               :ops waved-ops
               :waves (into (sorted-map) wave-groups)
               :wave-count (count wave-groups)})))
        (catch #?(:clj Exception :cljs :default) e
          {:valid false
           :errors [(#?(:clj .getMessage :cljs .-message) e)]})))))
