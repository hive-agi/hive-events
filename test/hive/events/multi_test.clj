(ns hive.events.multi-test
  "Tests for multimethod-based event dispatch.

   hive.events.multi provides an open-extension event dispatch system
   using Clojure multimethods. Unlike the atom-registry in router.cljc,
   multimethods allow any namespace to defmethod a handler without
   touching centralized state."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive.events.multi :as multi]
            [hive.events.fx :as fx]
            [hive.events.cofx :as cofx]
            [hive.events.interceptor :as interceptor]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defn clean-fx-fixture
  "Ensure fx/cofx registries are clean between tests."
  [f]
  (let [old-fx @(var-get #'fx/fx-registry)
        old-cofx @(var-get #'cofx/cofx-registry)]
    (try
      (f)
      (finally
        (reset! (var-get #'fx/fx-registry) old-fx)
        (reset! (var-get #'cofx/cofx-registry) old-cofx)))))

(use-fixtures :each clean-fx-fixture)

;; =============================================================================
;; Core: handle multimethod
;; =============================================================================

(deftest handle-dispatch-fn-test
  (testing "handle multimethod dispatches on first element of event vector"
    (is (= :test-event (multi/event-id [:test-event {:data 1}])))
    (is (= :foo (multi/event-id [:foo])))
    (is (= :bar/baz (multi/event-id [:bar/baz "arg1" "arg2"])))))

(deftest handle-basic-handler-test
  (testing "basic handler receives coeffects and event, returns effects map"
    ;; Register a handler via defmethod
    (multi/register-handler! :multi-test/basic
                             (fn [coeffects event]
                               {:test-result {:event-id (first event)
                                              :data (second event)}}))
    (let [ctx (multi/dispatch-sync [:multi-test/basic {:msg "hello"}])]
      (is (map? ctx) "dispatch-sync returns context")
      (is (= {:event-id :multi-test/basic
              :data {:msg "hello"}}
             (get-in ctx [:effects :test-result]))))))

(deftest handle-unknown-event-test
  (testing "dispatching an unregistered event throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"No handler"
                          (multi/dispatch-sync [:multi-test/nonexistent])))))

;; =============================================================================
;; Interceptor Integration
;; =============================================================================

(deftest interceptor-chain-test
  (testing "handlers can be wrapped with interceptors"
    (let [log (atom [])]
      (multi/register-handler! :multi-test/intercepted
                               [(interceptor/->interceptor
                                 :id :test-log
                                 :before (fn [ctx] (swap! log conj :before) ctx)
                                 :after (fn [ctx] (swap! log conj :after) ctx))]
                               (fn [coeffects event]
                                 (swap! log conj :handler)
                                 {:logged true}))
      (multi/dispatch-sync [:multi-test/intercepted])
      (is (= [:before :handler :after] @log)
          "Interceptors run before/after in correct order"))))

(deftest trim-v-interceptor-test
  (testing "trim-v interceptor removes event-id from event vector"
    (multi/register-handler! :multi-test/trimmed
                             [interceptor/trim-v]
                             (fn [coeffects event]
        ;; After trim-v, event should be just the args
                               {:trimmed-event event}))
    (let [ctx (multi/dispatch-sync [:multi-test/trimmed :arg1 :arg2])]
      (is (= [:arg1 :arg2]
             (get-in ctx [:effects :trimmed-event]))))))

(deftest multiple-interceptors-test
  (testing "multiple interceptors execute in correct order (LIFO for :after)"
    (let [log (atom [])]
      (multi/register-handler! :multi-test/multi-interceptors
                               [(interceptor/->interceptor
                                 :id :first
                                 :before (fn [ctx] (swap! log conj :first-before) ctx)
                                 :after (fn [ctx] (swap! log conj :first-after) ctx))
                                (interceptor/->interceptor
                                 :id :second
                                 :before (fn [ctx] (swap! log conj :second-before) ctx)
                                 :after (fn [ctx] (swap! log conj :second-after) ctx))]
                               (fn [coeffects event]
                                 (swap! log conj :handler)
                                 {}))
      (multi/dispatch-sync [:multi-test/multi-interceptors])
      ;; Before: first, second, handler
      ;; After: second, first (LIFO)
      (is (= [:first-before :second-before :handler :second-after :first-after] @log)))))

;; =============================================================================
;; Coeffect Injection
;; =============================================================================

(deftest cofx-injection-test
  (testing "coeffects are injected into handler context"
    (cofx/reg-cofx :test-timestamp
                   (fn [coeffects]
                     (assoc coeffects :test-timestamp 1234567890)))
    (multi/register-handler! :multi-test/with-cofx
                             [(cofx/inject-cofx :test-timestamp)]
                             (fn [coeffects event]
                               {:received-timestamp (:test-timestamp coeffects)}))
    (let [ctx (multi/dispatch-sync [:multi-test/with-cofx])]
      (is (= 1234567890
             (get-in ctx [:effects :received-timestamp]))))))

;; =============================================================================
;; Effect Execution
;; =============================================================================

(deftest effects-execution-test
  (testing "effects from handler are executed via fx system"
    (let [side-effect (atom nil)]
      (fx/reg-fx :test-side-effect
                 (fn [value]
                   (reset! side-effect value)))
      (multi/register-handler! :multi-test/with-fx
                               (fn [coeffects event]
                                 {:test-side-effect {:msg "effect fired"}}))
      (multi/dispatch-sync [:multi-test/with-fx])
      (is (= {:msg "effect fired"} @side-effect)))))

(deftest multiple-effects-test
  (testing "multiple effects from a single handler are all executed"
    (let [log (atom [])]
      (fx/reg-fx :test-log-fx
                 (fn [value]
                   (swap! log conj value)))
      (multi/register-handler! :multi-test/multi-fx
                               (fn [coeffects event]
                                 {:test-log-fx :first-effect
                                  :test-side-effect :second-effect}))
      (fx/reg-fx :test-side-effect
                 (fn [value]
                   (swap! log conj value)))
      (multi/dispatch-sync [:multi-test/multi-fx])
      (is (= 2 (count @log)) "Both effects should fire"))))

;; =============================================================================
;; Event Data Handling
;; =============================================================================

(deftest event-with-data-test
  (testing "handler receives full event vector including data"
    (multi/register-handler! :multi-test/echo
                             (fn [coeffects event]
                               {:echo event}))
    (let [ctx (multi/dispatch-sync [:multi-test/echo {:id 42} "extra"])]
      (is (= [:multi-test/echo {:id 42} "extra"]
             (get-in ctx [:effects :echo]))))))

(deftest event-without-data-test
  (testing "handler works with event vector containing only event-id"
    (multi/register-handler! :multi-test/no-data
                             (fn [coeffects event]
                               {:event-count (count event)}))
    (let [ctx (multi/dispatch-sync [:multi-test/no-data])]
      (is (= 1 (get-in ctx [:effects :event-count]))))))

;; =============================================================================
;; Handler Override
;; =============================================================================

(deftest handler-override-test
  (testing "re-registering a handler replaces the previous one"
    (multi/register-handler! :multi-test/override
                             (fn [coeffects event]
                               {:version 1}))
    (let [ctx1 (multi/dispatch-sync [:multi-test/override])]
      (is (= 1 (get-in ctx1 [:effects :version]))))
    ;; Override
    (multi/register-handler! :multi-test/override
                             (fn [coeffects event]
                               {:version 2}))
    (let [ctx2 (multi/dispatch-sync [:multi-test/override])]
      (is (= 2 (get-in ctx2 [:effects :version]))))))

;; =============================================================================
;; Handler Registration Query
;; =============================================================================

(deftest handler-registered-test
  (testing "can check if a handler is registered"
    (multi/register-handler! :multi-test/exists
                             (fn [_ _] {}))
    (is (true? (multi/handler-registered? :multi-test/exists)))
    (is (false? (multi/handler-registered? :multi-test/definitely-not-registered)))))

;; =============================================================================
;; Context Shape
;; =============================================================================

(deftest context-shape-test
  (testing "dispatch-sync returns context with :coeffects and :effects"
    (multi/register-handler! :multi-test/ctx-shape
                             (fn [coeffects event]
                               {:result :ok}))
    (let [ctx (multi/dispatch-sync [:multi-test/ctx-shape {:data 1}])]
      (is (contains? ctx :coeffects) "Context has :coeffects")
      (is (contains? ctx :effects) "Context has :effects")
      (is (= [:multi-test/ctx-shape {:data 1}]
             (get-in ctx [:coeffects :event]))
          "Coeffects contain original event"))))

;; =============================================================================
;; remove-handler!
;; =============================================================================

(deftest remove-handler-test
  (testing "remove-handler! makes dispatch throw for that event"
    (multi/register-handler! :multi-test/removable
                             (fn [_ _] {:ok true}))
    (is (some? (multi/dispatch-sync [:multi-test/removable])))
    (multi/remove-handler! :multi-test/removable)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"No handler"
                          (multi/dispatch-sync [:multi-test/removable])))))

;; =============================================================================
;; Pure Handler (no side effects)
;; =============================================================================

(deftest pure-handler-no-effects-test
  (testing "handler returning empty effects map is valid"
    (multi/register-handler! :multi-test/pure
                             (fn [coeffects event]
                               {}))
    (let [ctx (multi/dispatch-sync [:multi-test/pure])]
      (is (= {} (:effects ctx))))))

;; =============================================================================
;; Namespaced Keywords
;; =============================================================================

(deftest namespaced-event-id-test
  (testing "namespaced keywords work as event IDs"
    (multi/register-handler! :my.domain/create-user
                             (fn [coeffects event]
                               {:user-created (second event)}))
    (let [ctx (multi/dispatch-sync [:my.domain/create-user {:name "Alice"}])]
      (is (= {:name "Alice"}
             (get-in ctx [:effects :user-created]))))))

;; =============================================================================
;; Integration: Interceptors + Cofx + Fx
;; =============================================================================

(deftest full-integration-test
  (testing "full pipeline: cofx injection -> interceptors -> handler -> fx execution"
    (let [side-effects (atom [])
          log (atom [])]
      ;; Register coeffect
      (cofx/reg-cofx :request-id
                     (fn [coeffects]
                       (assoc coeffects :request-id "req-42")))
      ;; Register effect handler
      (fx/reg-fx :audit-log
                 (fn [value]
                   (swap! side-effects conj value)))
      ;; Register event with interceptors + cofx
      (multi/register-handler! :multi-test/full-pipeline
                               [(cofx/inject-cofx :request-id)
                                (interceptor/->interceptor
                                 :id :trace
                                 :before (fn [ctx] (swap! log conj :traced) ctx))]
                               (fn [coeffects event]
                                 {:audit-log {:request-id (:request-id coeffects)
                                              :action (second event)}}))
      ;; Dispatch
      (multi/dispatch-sync [:multi-test/full-pipeline :delete-user])
      ;; Verify
      (is (= [:traced] @log))
      (is (= [{:request-id "req-42" :action :delete-user}] @side-effects)))))

;; =============================================================================
;; Pure Orchestration: normalize-op
;; =============================================================================

(deftest normalize-op-string-keys-test
  (testing "converts string keys to keywords"
    (let [op (multi/normalize-op {"tool" "memory" "command" "add" "id" "op-1"})]
      (is (= "memory" (:tool op)))
      (is (= "add" (:command op)))
      (is (= "op-1" (:id op))))))

(deftest normalize-op-auto-id-test
  (testing "auto-generates :id when missing"
    (let [op (multi/normalize-op {:tool "memory" :command "add"})]
      (is (string? (:id op)))
      (is (clojure.string/starts-with? (:id op) "op-"))))

  (testing "auto-generates :id when blank"
    (let [op (multi/normalize-op {:tool "memory" :command "add" :id ""})]
      (is (clojure.string/starts-with? (:id op) "op-")))))

(deftest normalize-op-depends-on-test
  (testing "normalizes nil depends_on to empty vector"
    (let [op (multi/normalize-op {:tool "memory" :id "op-1"})]
      (is (= [] (:depends_on op)))))

  (testing "normalizes string depends_on to vector"
    (let [op (multi/normalize-op {:tool "memory" :id "op-1" :depends_on "op-0"})]
      (is (= ["op-0"] (:depends_on op)))))

  (testing "normalizes sequential depends_on to vector"
    (let [op (multi/normalize-op {:tool "memory" :id "op-1" :depends_on '("a" "b")})]
      (is (= ["a" "b"] (:depends_on op)))))

  (testing "preserves existing vector"
    (let [op (multi/normalize-op {:tool "memory" :id "op-1" :depends_on ["a"]})]
      (is (= ["a"] (:depends_on op))))))

(deftest normalize-op-preserves-params-test
  (testing "preserves extra parameters"
    (let [op (multi/normalize-op {:tool "memory" :command "add" :id "op-1"
                                  :content "hello" :type "note" :tags ["a" "b"]})]
      (is (= "hello" (:content op)))
      (is (= "note" (:type op)))
      (is (= ["a" "b"] (:tags op))))))

;; =============================================================================
;; Pure Orchestration: validate-ops
;; =============================================================================

(deftest validate-ops-valid-test
  (testing "valid ops return {:valid true}"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory" :command "add"}
                                      {:id "op-2" :tool "kg" :command "edge" :depends_on ["op-1"]}])]
      (is (true? (:valid result))))))

(deftest validate-ops-missing-id-test
  (testing "missing :id produces error"
    (let [result (multi/validate-ops [{:tool "memory" :command "add"}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "missing :id") (:errors result))))))

(deftest validate-ops-missing-tool-test
  (testing "missing :tool produces error"
    (let [result (multi/validate-ops [{:id "op-1" :command "add"}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "missing :tool") (:errors result))))))

(deftest validate-ops-duplicate-ids-test
  (testing "duplicate IDs produce error"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory"}
                                      {:id "op-1" :tool "kg"}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "Duplicate") (:errors result))))))

(deftest validate-ops-self-dep-test
  (testing "self-dependency produces error"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory" :depends_on ["op-1"]}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "depends on itself") (:errors result))))))

(deftest validate-ops-dangling-dep-test
  (testing "dependency on non-existent op produces error"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory" :depends_on ["op-999"]}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "non-existent") (:errors result))))))

(deftest validate-ops-circular-test
  (testing "circular dependency detected"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory" :depends_on ["op-2"]}
                                      {:id "op-2" :tool "kg" :depends_on ["op-1"]}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "Circular") (:errors result))))))

(deftest validate-ops-three-node-cycle-test
  (testing "three-node cycle detected"
    (let [result (multi/validate-ops [{:id "a" :tool "x" :depends_on ["c"]}
                                      {:id "b" :tool "x" :depends_on ["a"]}
                                      {:id "c" :tool "x" :depends_on ["b"]}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "Circular") (:errors result))))))

(deftest validate-ops-empty-deps-test
  (testing "empty depends_on is valid"
    (let [result (multi/validate-ops [{:id "op-1" :tool "memory" :depends_on []}])]
      (is (true? (:valid result))))))

;; =============================================================================
;; Pure Orchestration: expand-batch-macro
;; =============================================================================

(deftest expand-batch-non-batch-passthrough-test
  (testing "non-batch ops pass through unchanged"
    (let [ops [{:id "op-1" :tool "memory" :command "add" :content "hello"}]
          result (multi/expand-batch-macro ops)]
      (is (= 1 (count result)))
      (is (= "op-1" (:id (first result)))))))

(deftest expand-batch-fan-out-test
  (testing "batch with single array param fans out into sub-ops + join"
    (let [ops [{:id "op-1" :tool "memory" :command ["batch" "add"]
                :content ["note1" "note2" "note3"] :type "note"}]
          result (multi/expand-batch-macro ops)]
      ;; 3 sub-ops + 1 join = 4
      (is (= 4 (count result)))
      ;; Sub-ops have indexed IDs
      (is (= "op-1.0" (:id (nth result 0))))
      (is (= "op-1.1" (:id (nth result 1))))
      (is (= "op-1.2" (:id (nth result 2))))
      ;; Sub-ops have the actual command, not batch
      (is (= "add" (:command (nth result 0))))
      ;; Sub-ops have individual content values
      (is (= "note1" (:content (nth result 0))))
      (is (= "note2" (:content (nth result 1))))
      (is (= "note3" (:content (nth result 2))))
      ;; Scalar params broadcast
      (is (= "note" (:type (nth result 0))))
      (is (= "note" (:type (nth result 1))))
      ;; Join op
      (let [join (last result)]
        (is (= "op-1" (:id join)))
        (is (= :join (:command join)))
        (is (= ["op-1.0" "op-1.1" "op-1.2"] (:depends_on join)))))))

(deftest expand-batch-no-array-unwrap-test
  (testing "batch with no fan-out params just unwraps command"
    (let [ops [{:id "op-1" :tool "memory" :command ["batch" "add"]
                :content "scalar" :type "note"}]
          result (multi/expand-batch-macro ops)]
      (is (= 1 (count result)))
      (is (= "add" (:command (first result)))))))

(deftest expand-batch-known-array-exclusion-test
  (testing "known array params (tags, files, etc.) are NOT fanned out"
    (let [ops [{:id "op-1" :tool "memory" :command ["batch" "add"]
                :tags ["a" "b"] :content "hello" :type "note"}]
          result (multi/expand-batch-macro ops)]
      ;; tags is in known-array-params, so no fan-out
      (is (= 1 (count result)))
      (is (= "add" (:command (first result))))
      (is (= ["a" "b"] (:tags (first result)))))))

(deftest expand-batch-multiple-arrays-error-test
  (testing "multiple fan-out arrays throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"multiple array params"
         (multi/expand-batch-macro [{:id "op-1" :tool "memory"
                                     :command ["batch" "add"]
                                     :content ["a" "b"]
                                     :query ["x" "y"]}])))))

(deftest expand-batch-empty-array-error-test
  (testing "empty fan-out array throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"empty fan-out"
         (multi/expand-batch-macro [{:id "op-1" :tool "memory"
                                     :command ["batch" "add"]
                                     :content []}])))))

(deftest expand-batch-preserves-depends-on-test
  (testing "sub-ops inherit depends_on from parent batch op"
    (let [ops [{:id "op-1" :tool "memory" :command ["batch" "add"]
                :content ["a" "b"] :depends_on ["op-0"]}]
          result (multi/expand-batch-macro ops)]
      ;; Sub-ops should have parent's depends_on
      (is (= ["op-0"] (:depends_on (first result))))
      (is (= ["op-0"] (:depends_on (second result)))))))

;; =============================================================================
;; Pure Orchestration: assign-waves
;; =============================================================================

(deftest assign-waves-independent-test
  (testing "independent ops go in the same wave"
    (let [ops [{:id "a" :tool "x" :depends_on []}
               {:id "b" :tool "y" :depends_on []}
               {:id "c" :tool "z" :depends_on []}]
          result (multi/assign-waves ops)]
      (is (= 3 (count result)))
      (is (every? #(= 1 (:wave %)) result)))))

(deftest assign-waves-chain-test
  (testing "chain creates sequential waves"
    (let [ops [{:id "a" :tool "x" :depends_on []}
               {:id "b" :tool "y" :depends_on ["a"]}
               {:id "c" :tool "z" :depends_on ["b"]}]
          result (multi/assign-waves ops)
          by-id (into {} (map (juxt :id identity) result))]
      (is (= 1 (:wave (get by-id "a"))))
      (is (= 2 (:wave (get by-id "b"))))
      (is (= 3 (:wave (get by-id "c")))))))

(deftest assign-waves-diamond-test
  (testing "diamond dependency creates 3 waves"
    ;;     a
    ;;    / \
    ;;   b   c
    ;;    \ /
    ;;     d
    (let [ops [{:id "a" :tool "x" :depends_on []}
               {:id "b" :tool "x" :depends_on ["a"]}
               {:id "c" :tool "x" :depends_on ["a"]}
               {:id "d" :tool "x" :depends_on ["b" "c"]}]
          result (multi/assign-waves ops)
          by-id (into {} (map (juxt :id identity) result))]
      (is (= 1 (:wave (get by-id "a"))))
      (is (= 2 (:wave (get by-id "b"))))
      (is (= 2 (:wave (get by-id "c"))))
      (is (= 3 (:wave (get by-id "d")))))))

(deftest assign-waves-fan-out-same-wave-test
  (testing "fan-out sub-ops land in the same wave"
    (let [ops [{:id "op-1.0" :tool "memory" :depends_on []}
               {:id "op-1.1" :tool "memory" :depends_on []}
               {:id "op-1.2" :tool "memory" :depends_on []}
               {:id "op-1" :tool :noop :command :join :depends_on ["op-1.0" "op-1.1" "op-1.2"]}]
          result (multi/assign-waves ops)
          by-id (into {} (map (juxt :id identity) result))]
      ;; Sub-ops in wave 1, join in wave 2
      (is (= 1 (:wave (get by-id "op-1.0"))))
      (is (= 1 (:wave (get by-id "op-1.1"))))
      (is (= 1 (:wave (get by-id "op-1.2"))))
      (is (= 2 (:wave (get by-id "op-1")))))))

(deftest assign-waves-empty-input-test
  (testing "empty input returns empty result"
    (is (= [] (multi/assign-waves [])))))

(deftest assign-waves-single-op-test
  (testing "single op gets wave 1"
    (let [result (multi/assign-waves [{:id "only" :tool "x" :depends_on []}])]
      (is (= 1 (count result)))
      (is (= 1 (:wave (first result)))))))

;; =============================================================================
;; Pure Orchestration: compile-multi-spec
;; =============================================================================

(deftest compile-multi-spec-basic-test
  (testing "compile-multi-spec returns valid spec for simple ops"
    (let [result (multi/compile-multi-spec
                  [{:id "op-1" :tool "memory" :command "add" :content "hello"}
                   {:id "op-2" :tool "kg" :command "edge" :depends_on ["op-1"]}])]
      (is (true? (:valid result)))
      (is (= 2 (count (:ops result))))
      (is (= 2 (:wave-count result)))
      (is (contains? (:waves result) 1))
      (is (contains? (:waves result) 2)))))

(deftest compile-multi-spec-validation-error-test
  (testing "compile-multi-spec catches validation errors"
    (let [result (multi/compile-multi-spec
                  [{:id "op-1" :tool "memory" :depends_on ["op-2"]}
                   {:id "op-2" :tool "kg" :depends_on ["op-1"]}])]
      (is (false? (:valid result)))
      (is (seq (:errors result))))))

(deftest compile-multi-spec-auto-normalize-test
  (testing "compile-multi-spec normalizes string keys"
    (let [result (multi/compile-multi-spec
                  [{"tool" "memory" "command" "add" "id" "op-1"}])]
      (is (true? (:valid result)))
      (is (= 1 (count (:ops result))))
      (is (= "memory" (:tool (first (:ops result))))))))

(deftest compile-multi-spec-auto-id-test
  (testing "compile-multi-spec auto-generates IDs"
    (let [result (multi/compile-multi-spec
                  [{:tool "memory" :command "add"}
                   {:tool "kg" :command "edge"}])]
      (is (true? (:valid result)))
      (is (= 2 (count (:ops result))))
      ;; All ops should have wave 1 (independent)
      (is (= 1 (:wave-count result))))))

(deftest compile-multi-spec-batch-expansion-test
  (testing "compile-multi-spec expands batch operations"
    (let [result (multi/compile-multi-spec
                  [{:id "op-1" :tool "memory" :command ["batch" "add"]
                    :content ["a" "b"] :type "note"}])]
      (is (true? (:valid result)))
      ;; 2 sub-ops + 1 join = 3 ops total
      (is (= 3 (count (:ops result))))
      ;; 2 waves: sub-ops in wave 1, join in wave 2
      (is (= 2 (:wave-count result))))))

(deftest compile-multi-spec-empty-ops-test
  (testing "compile-multi-spec with empty ops returns valid with 0 waves"
    (let [result (multi/compile-multi-spec [])]
      (is (true? (:valid result)))
      (is (= 0 (count (:ops result))))
      (is (= 0 (:wave-count result))))))

(deftest compile-multi-spec-batch-error-test
  (testing "compile-multi-spec catches batch expansion errors"
    (let [result (multi/compile-multi-spec
                  [{:id "op-1" :tool "memory" :command ["batch" "add"]
                    :content ["a" "b"] :query ["x" "y"]}])]
      (is (false? (:valid result)))
      (is (some #(clojure.string/includes? % "multiple array params") (:errors result))))))
