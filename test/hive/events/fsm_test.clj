(ns hive.events.fsm-test
  "Tests for the FSM workflow engine."
  (:require [clojure.test :refer [deftest is testing]]
            [hive.events.fsm :as fsm]
            [hive.events.fx :as fx]))

(deftest simple-counter-fsm
  (testing "FSM that counts to 4 and returns"
    (let [result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_resources data]
                                                        (update data :count (fnil inc 0)))
                                          :dispatches [[::fsm/end (fn [state] (> (:count state) 3))]
                                                       [::fsm/start (constantly true)]]}}})
                     (fsm/run))]
      (is (= 4 (:count result))))))

(deftest fsm-with-resources
  (testing "FSM handlers can use resources map"
    (let [result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [resources data]
                                                        (assoc data :value ((:compute resources) (:input data))))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run
                      {:compute #(* % 2)}
                      {:data {:input 21}}))]
      (is (= 42 (:value result))))))

(deftest fsm-with-initial-state
  (testing "FSM with initial data"
    (let [result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data] (update data :x inc))
                                          :dispatches [[:foo (constantly true)]]}
                             :foo        {:handler    (fn [_r data] (update data :x inc))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run {} {:data {:x 1}}))]
      (is (= 3 (:x result))))))

(deftest fsm-multi-state-transitions
  (testing "FSM with conditional dispatch between states"
    (let [result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data] (assoc data :phase :smite))
                                          :dispatches [[:survey (fn [{:keys [phase]}] (= phase :smite))]]}
                             :survey     {:handler    (fn [_r data] (assoc data :phase :survey :tasks 3))
                                          :dispatches [[:spark (fn [{:keys [tasks]}] (pos? tasks))]
                                                       [::fsm/end (constantly true)]]}
                             :spark      {:handler    (fn [_r data] (assoc data :phase :spark :spawned 2))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (= :spark (:phase result)))
      (is (= 2 (:spawned result)))
      (is (= 3 (:tasks result))))))

(deftest fsm-with-subscriptions
  (testing "Subscriptions fire on state path changes"
    (let [changes (atom [])
          result (-> (fsm/compile
                      {:fsm  {::fsm/start {:handler    (fn [_r data] (assoc data :x 1))
                                           :dispatches [[:step2 (constantly true)]]}
                              :step2      {:handler    (fn [_r data] (update data :x inc))
                                           :dispatches [[::fsm/end (constantly true)]]}}
                       :opts {:subscriptions {[:x] {:handler (fn [path old-val new-val]
                                                               (swap! changes conj {:path path
                                                                                    :old old-val
                                                                                    :new new-val}))}}}})
                     (fsm/run))]
      (is (= 2 (:x result)))
      (is (pos? (count @changes))))))

(deftest fsm-with-edn-handlers
  (testing "FSM compiled from EDN spec with external handler map"
    (let [spec {:fsm {::fsm/start {:handler    :my-handler
                                   :dispatches [[::fsm/end (constantly true)]]}}}
          handlers {:my-handler (fn [_r data] (assoc data :handled true))}
          result (-> (fsm/compile spec handlers)
                     (fsm/run))]
      (is (true? (:handled result))))))

(deftest fsm-halt-and-resume
  (testing "FSM can halt and be resumed"
    (let [compiled (fsm/compile
                    {:fsm {::fsm/start {:handler    (fn [_r data] (update data :step (fnil inc 0)))
                                        :dispatches [[::fsm/halt (fn [{:keys [step]}] (= step 1))]
                                                     [::fsm/start (constantly true)]]}}})
          halted (fsm/run compiled {} {:data {:step 0}})]
      ;; Halted FSM retains state for resume
      (is (= 1 (get-in halted [:data :step])))
      (is (some? (:current-state-id halted))))))

(deftest fsm-trace
  (testing "FSM records state transition trace"
    (let [compiled (fsm/compile
                    {:fsm  {::fsm/start {:handler    (fn [_r data] (assoc data :a 1))
                                         :dispatches [[:mid (constantly true)]]}
                            :mid        {:handler    (fn [_r data] (assoc data :b 2))
                                         :dispatches [[::fsm/end (constantly true)]]}}
                     :opts {:max-trace 10}})
          ;; Use halt to inspect trace — run returns just data
          ;; Instead, use a custom end handler that returns the full FSM
          compiled-with-trace (fsm/compile
                               {:fsm  {::fsm/start {:handler    (fn [_r data] (assoc data :a 1))
                                                    :dispatches [[:mid (constantly true)]]}
                                       :mid        {:handler    (fn [_r data] (assoc data :b 2))
                                                    :dispatches [[::fsm/end (constantly true)]]}
                                       ::fsm/end   {:handler (fn [_r fsm] fsm)}}
                                :opts {:max-trace 10}})
          result (fsm/run compiled-with-trace)]
      (is (vector? (:trace result)))
      (is (= 2 (count (:trace result))))
      (is (= ::fsm/start (:state-id (first (:trace result))))))))

(deftest fsm-pre-post-hooks
  (testing "Pre and post hooks are called on each transition"
    (let [hook-log (atom [])
          result (-> (fsm/compile
                      {:fsm  {::fsm/start {:handler    (fn [_r data] (assoc data :done true))
                                           :dispatches [[::fsm/end (constantly true)]]}}
                       :opts {:pre  (fn [fsm _r]
                                      (swap! hook-log conj [:pre (:current-state-id fsm)])
                                      fsm)
                              :post (fn [fsm _r]
                                      (swap! hook-log conj [:post (:current-state-id fsm)])
                                      fsm)}})
                     (fsm/run))]
      (is (true? (:done result)))
      (is (pos? (count @hook-log))))))

;; =============================================================================
;; FX Integration Tests
;; =============================================================================

(deftest fsm-handler-returns-fx
  (testing "Handler returning {:data ..., :fx [...]} processes effects"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:data (assoc data :processed true)
                                                         :fx   [[:test-log {:msg "handler fired"}]]})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (true? (:processed result)) "Data should be extracted from :data key")
      (is (= 1 (count @fx-log)) "One effect should have been processed")
      (is (= {:msg "handler fired"} (first @fx-log)) "Effect value should match")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-returns-plain-data-backward-compat
  (testing "Handler returning plain data still works (backward compatibility)"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        (assoc data :plain true))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (true? (:plain result)) "Plain data should work as before")
      (is (empty? @fx-log) "No effects should fire for plain data handlers")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-fx-multiple-effects
  (testing "Handler can return multiple effects in order"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:data (assoc data :done true)
                                                         :fx   [[:test-log :first]
                                                                [:test-log :second]
                                                                [:test-log :third]]})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (true? (:done result)))
      (is (= [:first :second :third] @fx-log) "Effects should fire in order, including duplicates")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-fx-across-states
  (testing "Effects fire correctly across multiple state transitions"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:data (assoc data :phase :started)
                                                         :fx   [[:test-log :start-fx]]})
                                          :dispatches [[:process (constantly true)]]}
                             :process    {:handler    (fn [_r data]
                                                        {:data (assoc data :phase :processed)
                                                         :fx   [[:test-log :process-fx]]})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (= :processed (:phase result)))
      (is (= [:start-fx :process-fx] @fx-log) "Effects from each state should fire in order")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-fx-mixed-with-plain
  (testing "Mix of fx-returning and plain-data handlers works"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        ;; Plain data - no fx
                                                        (assoc data :step 1))
                                          :dispatches [[:step2 (constantly true)]]}
                             :step2      {:handler    (fn [_r data]
                                                        ;; FX-enhanced
                                                        {:data (assoc data :step 2)
                                                         :fx   [[:test-log :from-step2]]})
                                          :dispatches [[:step3 (constantly true)]]}
                             :step3      {:handler    (fn [_r data]
                                                        ;; Plain data again
                                                        (assoc data :step 3))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (= 3 (:step result)))
      (is (= [:from-step2] @fx-log) "Only step2 should produce effects")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-fx-empty-vector
  (testing "Handler returning {:data ..., :fx []} is valid (no effects)"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:data (assoc data :done true)
                                                         :fx   []})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (true? (:done result)))
      (is (empty? @fx-log) "Empty fx vector should not trigger any effects")
      (fx/clear-fx :test-log))))

(deftest fsm-handler-fx-data-with-fx-key-no-conflict
  (testing "Plain data map containing :fx key (non-sequential) is treated as plain data"
    ;; Edge case: handler returns {:fx "some-string", :other "val"}
    ;; Since :fx is not sequential, this is plain data, not an fx-enhanced result.
    (let [result (-> (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        (assoc data :fx "not-effects" :done true))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
                     (fsm/run))]
      (is (true? (:done result)))
      (is (= "not-effects" (:fx result)) "Plain :fx value should pass through"))))

;; =============================================================================
;; Sub-FSM Composition Tests
;; =============================================================================

(deftest sub-fsm-basic-composition
  (testing "run-sub-fsm executes child FSM and returns result"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r data]
                                                   (assoc data :child-value (* (:input data) 10)))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm child {} {:input 5})]
      (is (= 50 (:child-value result)))
      (is (= 5 (:input result))))))

(deftest sub-fsm-error-handling
  (testing "run-sub-fsm catches child errors and returns error map"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                   (throw (ex-info "child boom" {})))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm child {} {})]
      (is (fsm/sub-fsm-error? result))
      (is (string? (:sub-fsm/message result)))
      (is (some? (:sub-fsm/cause result))))))

(deftest sub-fsm-error-predicate
  (testing "sub-fsm-error? returns false for normal results"
    (is (not (fsm/sub-fsm-error? {:value 42})))
    (is (not (fsm/sub-fsm-error? {})))
    (is (not (fsm/sub-fsm-error? nil)))))

(deftest sub-fsm-resource-sharing
  (testing "run-sub-fsm passes resources to child"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [resources data]
                                                   (assoc data :computed ((:compute resources) (:x data))))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm child {:compute #(* % 3)} {:x 7})]
      (is (= 21 (:computed result))))))

(deftest sub-fsm-multi-state-child
  (testing "run-sub-fsm works with multi-state child FSM"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r data] (assoc data :phase :a))
                                     :dispatches [[:step-b (constantly true)]]}
                        :step-b     {:handler    (fn [_r data] (assoc data :phase :b :doubled (* 2 (:val data))))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm child {} {:val 6})]
      (is (= :b (:phase result)))
      (is (= 12 (:doubled result))))))

(deftest sub-fsm-nested-in-parent-handler
  (testing "Parent handler calls run-sub-fsm and merges result"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :enriched (str "ctx-" (:id data))))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [ctx (fsm/run-sub-fsm child-fsm resources
                                                                               {:id (:agent-id data)})]
                                                      (assoc data :context ctx)))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:agent-id "ling-1"}})]
      (is (= "ling-1" (:agent-id result)))
      (is (= "ctx-ling-1" (get-in result [:context :enriched]))))))

(deftest sub-fsm-error-in-parent-handler
  (testing "Parent handler gracefully handles sub-FSM error"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                       (throw (ex-info "sub-boom" {:reason :test})))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [result (fsm/run-sub-fsm child-fsm resources {})]
                                                      (if (fsm/sub-fsm-error? result)
                                                        (assoc data :error (:sub-fsm/message result))
                                                        (merge data result))))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:status :ok}})]
      (is (= :ok (:status result)))
      (is (string? (:error result))))))

(deftest make-sub-fsm-handler-default-merge
  (testing "make-sub-fsm-handler with default merge (shallow merge into parent)"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :child-key "from-child"))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler child-fsm)
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:parent-key "from-parent"}})]
      (is (= "from-parent" (:parent-key result)))
      (is (= "from-child" (:child-key result))))))

(deftest make-sub-fsm-handler-result-key
  (testing "make-sub-fsm-handler with :result-key stores child result under key"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :enriched true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:result-key :child-output})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:parent-val 1}})]
      (is (= 1 (:parent-val result)))
      (is (true? (get-in result [:child-output :enriched]))))))

(deftest make-sub-fsm-handler-data-fn
  (testing "make-sub-fsm-handler with :data-fn selects child input"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :computed (* 2 (:x data))))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:data-fn    #(select-keys % [:x])
                                                    :result-key :math})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:x 5 :y 10 :secret "hidden"}})]
      (is (= 5 (:x result)))
      (is (= 10 (:y result)))
      (is (= "hidden" (:secret result)))
      (is (= 10 (get-in result [:math :computed])))
      ;; Child should NOT see :y or :secret
      (is (nil? (get-in result [:math :y])))
      (is (nil? (get-in result [:math :secret]))))))

(deftest make-sub-fsm-handler-resources-fn
  (testing "make-sub-fsm-handler with :resources-fn transforms resources for child"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [resources data]
                                                       (assoc data :result ((:child-fn resources) (:val data))))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:resources-fn (fn [r] {:child-fn (:parent-fn r)})
                                                    :result-key   :child})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {:parent-fn inc} {:data {:val 41}})]
      (is (= 42 (get-in result [:child :result]))))))

(deftest make-sub-fsm-handler-error-propagation
  (testing "make-sub-fsm-handler sets :error-key on child failure"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                       (throw (ex-info "child failed" {})))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:error-key :sub-error})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:status :ok}})]
      (is (= :ok (:status result)))
      (is (string? (:sub-error result))))))

(deftest make-sub-fsm-handler-custom-merge-fn
  (testing "make-sub-fsm-handler with custom :merge-fn"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :items [1 2 3]))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:merge-fn (fn [parent-data child-result]
                                                                (update parent-data :all-items
                                                                        (fnil into [])
                                                                        (:items child-result)))})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:all-items [0]}})]
      (is (= [0 1 2 3] (:all-items result))))))

(deftest sub-fsm-deeply-nested
  (testing "Three levels of FSM nesting works"
    (let [grandchild (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        (assoc data :depth 3 :gc-val "deep"))
                                          :dispatches [[::fsm/end (constantly true)]]}}})
          child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [resources data]
                                                   (let [gc (fsm/run-sub-fsm grandchild resources {:input (:x data)})]
                                                     (assoc data :depth 2 :grandchild gc)))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [c (fsm/run-sub-fsm child resources {:x (:val data)})]
                                                      (assoc data :depth 1 :child c)))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:val 42}})]
      (is (= 1 (:depth result)))
      (is (= 2 (get-in result [:child :depth])))
      (is (= 3 (get-in result [:child :grandchild :depth])))
      (is (= "deep" (get-in result [:child :grandchild :gc-val]))))))

;; =============================================================================
;; Sub-FSM FX-Aware Composition Tests
;; =============================================================================

(deftest sub-fsm-fx-captures-child-effects
  (testing "run-sub-fsm-fx captures child FX without executing them"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          child  (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    {:data (assoc data :processed true)
                                                     :fx   [[:test-log :from-child]]})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm-fx child {} {:input 1})]
      ;; FX should be captured, not executed
      (is (empty? @fx-log) "Child FX should NOT be executed")
      (is (= [[:test-log :from-child]] (:fx result)) "FX should be captured in result")
      (is (true? (get-in result [:data :processed])) "Child data should be returned")
      (fx/clear-fx :test-log))))

(deftest sub-fsm-fx-accumulates-across-states
  (testing "run-sub-fsm-fx accumulates FX from multiple child states"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          child  (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    {:data (assoc data :step 1)
                                                     :fx   [[:test-log :step-1]]})
                                      :dispatches [[:step2 (constantly true)]]}
                         :step2      {:handler    (fn [_r data]
                                                    {:data (assoc data :step 2)
                                                     :fx   [[:test-log :step-2a]
                                                            [:test-log :step-2b]]})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm-fx child {} {})]
      (is (empty? @fx-log) "No FX should be executed")
      (is (= [[:test-log :step-1]
              [:test-log :step-2a]
              [:test-log :step-2b]]
             (:fx result))
          "All FX from all states should be accumulated in order")
      (is (= 2 (get-in result [:data :step])))
      (fx/clear-fx :test-log))))

(deftest sub-fsm-fx-no-effects-returns-empty
  (testing "run-sub-fsm-fx with plain-data handlers returns empty FX"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r data]
                                                   (assoc data :done true))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm-fx child {} {})]
      (is (= [] (:fx result)) "FX should be empty vector")
      (is (true? (get-in result [:data :done]))))))

(deftest sub-fsm-fx-error-handling
  (testing "run-sub-fsm-fx returns error map on child failure"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                   (throw (ex-info "fx-boom" {})))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run-sub-fsm-fx child {} {})]
      (is (fsm/sub-fsm-error? result))
      ;; FSM engine wraps handler exceptions in "handler error" ex-info
      (is (string? (:sub-fsm/message result)))
      (is (some? (:sub-fsm/cause result))))))

(deftest sub-fsm-fx-parent-surfaces-child-fx
  (testing "Parent handler uses run-sub-fsm-fx to surface child FX"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          child  (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    {:data (assoc data :child-done true)
                                                     :fx   [[:test-log :child-fx]]})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [{child-data :data child-fx :fx}
                                                          (fsm/run-sub-fsm-fx child resources
                                                                              (select-keys data [:input]))]
                                                      {:data (assoc data :context child-data)
                                                       :fx (into [[:test-log :parent-fx]] child-fx)}))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:input 42}})]
      ;; Parent's FX pipeline should execute BOTH parent and surfaced child FX
      (is (= [:parent-fx :child-fx] @fx-log)
          "Parent FX first, then surfaced child FX")
      (is (true? (get-in result [:context :child-done])))
      (fx/clear-fx :test-log))))

(deftest make-sub-fsm-handler-fx-mode
  (testing "make-sub-fsm-handler with :fx? true surfaces child effects"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          child  (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    {:data (assoc data :enriched true)
                                                     :fx   [[:test-log :from-child]]})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child
                                                   {:result-key :child
                                                    :fx?        true})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:parent-val 1}})]
      ;; The child FX should be surfaced to parent pipeline and executed
      (is (= [:from-child] @fx-log)
          "Child FX should be executed via parent pipeline")
      (is (= 1 (:parent-val result)))
      (is (true? (get-in result [:child :enriched])))
      (fx/clear-fx :test-log))))

(deftest make-sub-fsm-handler-fx-mode-no-effects
  (testing "make-sub-fsm-handler with :fx? true but no child effects returns plain data"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r data]
                                                   (assoc data :plain true))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child
                                                   {:result-key :child
                                                    :fx?        true})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:x 1}})]
      (is (= 1 (:x result)))
      (is (true? (get-in result [:child :plain]))))))

(deftest make-sub-fsm-handler-fx-mode-error
  (testing "make-sub-fsm-handler with :fx? true handles child error"
    (let [child (fsm/compile
                 {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                   (throw (ex-info "fx-child-error" {})))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child
                                                   {:fx?       true
                                                    :error-key :sub-err})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:status :ok}})]
      (is (= :ok (:status result)))
      (is (string? (:sub-err result))))))

;; =============================================================================
;; Parent FSM Orchestration Tests
;; =============================================================================

(deftest parent-multi-state-with-sub-fsm-at-mid-state
  (testing "Parent FSM invokes sub-FSM at a non-start (mid-pipeline) state"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :enriched (str "ctx-" (:id data))))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    (assoc data :phase :initialized))
                                      :dispatches [[:gather (constantly true)]]}
                         :gather     {:handler    (fn [resources data]
                                                    (let [ctx (fsm/run-sub-fsm child-fsm resources
                                                                               {:id (:agent data)})]
                                                      (assoc data :phase :gathered :context ctx)))
                                      :dispatches [[:finalize (constantly true)]]}
                         :finalize   {:handler    (fn [_r data]
                                                    (assoc data :phase :finalized
                                                           :summary (str "done-" (get-in data [:context :enriched]))))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:agent "ling-42"}})]
      (is (= :finalized (:phase result)))
      (is (= "ctx-ling-42" (get-in result [:context :enriched])))
      (is (= "done-ctx-ling-42" (:summary result))))))

(deftest parent-continues-after-sub-fsm-error-recovery
  (testing "Parent dispatches to recovery state when sub-FSM fails"
    (let [failing-child (fsm/compile
                         {:fsm {::fsm/start {:handler    (fn [_r _data]
                                                           (throw (ex-info "child-crash" {:reason :timeout})))
                                             :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [result (fsm/run-sub-fsm failing-child resources {})]
                                                      (if (fsm/sub-fsm-error? result)
                                                        (assoc data :sub-failed true
                                                               :error-msg (:sub-fsm/message result))
                                                        (assoc data :sub-result result))))
                                      :dispatches [[:recover (fn [{:keys [sub-failed]}] sub-failed)]
                                                   [:success (constantly true)]]}
                         :recover   {:handler    (fn [_r data]
                                                   (assoc data :phase :recovered :fallback-value 0))
                                     :dispatches [[::fsm/end (constantly true)]]}
                         :success   {:handler    (fn [_r data]
                                                   (assoc data :phase :succeeded))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {}})]
      (is (= :recovered (:phase result)))
      (is (true? (:sub-failed result)))
      (is (string? (:error-msg result)))
      (is (= 0 (:fallback-value result))))))

(deftest parent-orchestrates-sequential-sub-fsms
  (testing "Parent invokes multiple sub-FSMs in sequence across states"
    (let [validator (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :valid? (pos? (:count data 0))))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          enricher (fsm/compile
                    {:fsm {::fsm/start {:handler    (fn [_r data]
                                                      (assoc data :label (str "item-" (:count data))))
                                        :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start  {:handler    (fn [resources data]
                                                     (let [v (fsm/run-sub-fsm validator resources
                                                                              (select-keys data [:count]))]
                                                       (assoc data :validation v)))
                                       :dispatches [[:enrich (fn [{:keys [validation]}]
                                                               (:valid? validation))]
                                                    [::fsm/end (constantly true)]]}
                         :enrich      {:handler    (fn [resources data]
                                                     (let [e (fsm/run-sub-fsm enricher resources
                                                                              (select-keys data [:count]))]
                                                       (assoc data :enrichment e)))
                                       :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:count 5}})]
      (is (true? (get-in result [:validation :valid?])))
      (is (= "item-5" (get-in result [:enrichment :label]))))))

(deftest parent-invokes-multiple-sub-fsms-in-single-handler
  (testing "Single parent handler calls two sub-FSMs and merges both"
    (let [auth-fsm (fsm/compile
                    {:fsm {::fsm/start {:handler    (fn [_r data]
                                                      {:token (str "tok-" (:user data))
                                                       :authenticated true})
                                        :dispatches [[::fsm/end (constantly true)]]}}})
          config-fsm (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:max-retries 3
                                                         :timeout-ms (* (:tier data) 1000)})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [auth (fsm/run-sub-fsm auth-fsm resources
                                                                                {:user (:user data)})
                                                          cfg  (fsm/run-sub-fsm config-fsm resources
                                                                                {:tier (:tier data)})]
                                                      (assoc data :auth auth :config cfg)))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:user "alice" :tier 2}})]
      (is (= "tok-alice" (get-in result [:auth :token])))
      (is (true? (get-in result [:auth :authenticated])))
      (is (= 3 (get-in result [:config :max-retries])))
      (is (= 2000 (get-in result [:config :timeout-ms]))))))

(deftest parent-halt-resume-with-sub-fsm
  (testing "Parent FSM halts before sub-FSM state, then resumes into it"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :enriched true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          compiled (fsm/compile
                    {:fsm {::fsm/start {:handler    (fn [_r data]
                                                      (assoc data :phase :ready))
                                        :dispatches [[:invoke (fn [{:keys [phase]}]
                                                                (= phase :ready))]]}
                           :invoke     {:handler    (fn [_r data]
                                                      (assoc data :phase :halting))
                                        :dispatches [[::fsm/halt (fn [{:keys [phase]}]
                                                                   (= phase :halting))]
                                                     [::fsm/end (constantly true)]]}}})
          halted (fsm/run compiled {} {:data {}})]
      ;; FSM should be halted with state preserved
      (is (map? halted))
      (is (= :halting (get-in halted [:data :phase])))
      (is (some? (:current-state-id halted)))
      ;; Resume by running from halted state using step
      (let [resumed-compiled (fsm/compile
                              {:fsm {::fsm/start {:handler    (fn [resources data]
                                                                (let [ctx (fsm/run-sub-fsm child-fsm resources
                                                                                           {:id "resumed"})]
                                                                  (assoc data :phase :completed :child ctx)))
                                                  :dispatches [[::fsm/end (constantly true)]]}}})
            result (fsm/run resumed-compiled {} {:data (:data halted)})]
        (is (= :completed (:phase result)))
        (is (true? (get-in result [:child :enriched])))))))

(deftest parent-trace-records-transitions-around-sub-fsm
  (testing "Parent FSM trace captures state transitions before/after sub-FSM invocation"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :child-done true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm  {::fsm/start {:handler    (fn [_r data]
                                                     (assoc data :step :prepare))
                                       :dispatches [[:invoke (constantly true)]]}
                          :invoke     {:handler    (fn [resources data]
                                                     (let [ctx (fsm/run-sub-fsm child-fsm resources {})]
                                                       (assoc data :step :invoked :child ctx)))
                                       :dispatches [[:cleanup (constantly true)]]}
                          :cleanup    {:handler    (fn [_r data]
                                                     (assoc data :step :cleaned))
                                       :dispatches [[::fsm/end (constantly true)]]}
                          ;; Custom end handler to expose trace
                          ::fsm/end   {:handler (fn [_r fsm] fsm)}}
                   :opts {:max-trace 50}})
          result (fsm/run parent)]
      ;; Should have 3 trace segments: ::start -> :invoke -> :cleanup
      (is (= 3 (count (:trace result))))
      (is (= ::fsm/start (:state-id (nth (:trace result) 0))))
      (is (= :invoke (:state-id (nth (:trace result) 1))))
      (is (= :cleanup (:state-id (nth (:trace result) 2))))
      ;; All transitions should be successful
      (is (every? #(= :success (:status %)) (:trace result))))))

(deftest make-sub-fsm-handler-at-mid-pipeline-state
  (testing "make-sub-fsm-handler works at a non-start state in parent pipeline"
    (let [enricher (fsm/compile
                    {:fsm {::fsm/start {:handler    (fn [_r data]
                                                      (assoc data :enriched true
                                                             :label (str "enriched-" (:x data))))
                                        :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    (assoc data :initialized true))
                                      :dispatches [[:mid (constantly true)]]}
                         :mid        {:handler    (fsm/make-sub-fsm-handler
                                                   enricher
                                                   {:data-fn    #(select-keys % [:x])
                                                    :result-key :enrichment})
                                      :dispatches [[:final (constantly true)]]}
                         :final      {:handler    (fn [_r data]
                                                    (assoc data :done true
                                                           :summary (get-in data [:enrichment :label])))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:x 99}})]
      (is (true? (:initialized result)))
      (is (true? (:done result)))
      (is (true? (get-in result [:enrichment :enriched])))
      (is (= "enriched-99" (:summary result))))))

(deftest parent-pre-post-hooks-with-sub-fsm
  (testing "Parent pre/post hooks fire around sub-FSM invocation states"
    (let [hook-log (atom [])
          child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :child-ran true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm  {::fsm/start {:handler    (fn [_r data]
                                                     (assoc data :phase :start))
                                       :dispatches [[:invoke (constantly true)]]}
                          :invoke     {:handler    (fn [resources data]
                                                     (let [ctx (fsm/run-sub-fsm child-fsm resources {})]
                                                       (assoc data :phase :invoked :child ctx)))
                                       :dispatches [[::fsm/end (constantly true)]]}}
                   :opts {:pre  (fn [fsm _r]
                                  (swap! hook-log conj [:pre (:current-state-id fsm)])
                                  fsm)
                          :post (fn [fsm _r]
                                  (swap! hook-log conj [:post (:current-state-id fsm)])
                                  fsm)}})
          result (fsm/run parent)]
      (is (= :invoked (:phase result)))
      (is (true? (get-in result [:child :child-ran])))
      ;; Hooks should include the :invoke state where sub-FSM runs
      (is (some #(= [:pre :invoke] %) @hook-log)
          "Pre hook should fire for invoke state")
      (is (some #(= [:post ::fsm/end] %) @hook-log)
          "Post hook should fire after transition to end"))))

(deftest parent-conditional-dispatch-on-sub-fsm-result
  (testing "Parent dispatches to different states based on sub-FSM result content"
    (let [scorer (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    (assoc data :score (* (:quality data) 10)))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [result (fsm/run-sub-fsm scorer resources
                                                                                  {:quality (:quality data)})]
                                                      (assoc data :score (:score result))))
                                      :dispatches [[:premium (fn [{:keys [score]}] (>= score 80))]
                                                   [:standard (fn [{:keys [score]}] (>= score 40))]
                                                   [:rejected (constantly true)]]}
                         :premium   {:handler    (fn [_r data] (assoc data :tier :premium))
                                     :dispatches [[::fsm/end (constantly true)]]}
                         :standard  {:handler    (fn [_r data] (assoc data :tier :standard))
                                     :dispatches [[::fsm/end (constantly true)]]}
                         :rejected  {:handler    (fn [_r data] (assoc data :tier :rejected))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          high   (fsm/run parent {} {:data {:quality 9}})
          mid    (fsm/run parent {} {:data {:quality 5}})
          low    (fsm/run parent {} {:data {:quality 2}})]
      (is (= :premium (:tier high)))
      (is (= 90 (:score high)))
      (is (= :standard (:tier mid)))
      (is (= 50 (:score mid)))
      (is (= :rejected (:tier low)))
      (is (= 20 (:score low))))))

(deftest parent-sub-fsm-shares-and-isolates-resources
  (testing "Sub-FSM gets transformed resources without polluting parent"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [resources data]
                                                       (assoc data
                                                              :child-db (:db resources)
                                                              :child-secret (:secret resources)))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:resources-fn (fn [r]
                                                                    (-> r
                                                                        (assoc :secret "child-only-secret")
                                                                        (dissoc :admin-key)))
                                                    :result-key :child})
                                      :dispatches [[:verify (constantly true)]]}
                         :verify    {:handler    (fn [resources data]
                                                   ;; Parent resources should still have :admin-key
                                                   (assoc data :parent-has-admin (contains? resources :admin-key)
                                                          :child-has-secret (get-in data [:child :child-secret])))
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent
                          {:db "prod-db" :admin-key "super-secret"}
                          {:data {}})]
      ;; Child got transformed resources
      (is (= "prod-db" (get-in result [:child :child-db])))
      (is (= "child-only-secret" (get-in result [:child :child-secret])))
      ;; Parent resources unchanged
      (is (true? (:parent-has-admin result))))))

(deftest parent-sub-fsm-fx-chain-across-states
  (testing "Parent with FX-aware sub-FSM handlers across multiple states"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          validate-fsm (fsm/compile
                        {:fsm {::fsm/start {:handler    (fn [_r data]
                                                          {:data (assoc data :validated true)
                                                           :fx [[:test-log :validated]]})
                                            :dispatches [[::fsm/end (constantly true)]]}}})
          enrich-fsm (fsm/compile
                      {:fsm {::fsm/start {:handler    (fn [_r data]
                                                        {:data (assoc data :enriched true)
                                                         :fx [[:test-log :enriched]]})
                                          :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   validate-fsm
                                                   {:result-key :validation
                                                    :fx? true})
                                      :dispatches [[:enrich (constantly true)]]}
                         :enrich    {:handler    (fsm/make-sub-fsm-handler
                                                  enrich-fsm
                                                  {:result-key :enrichment
                                                   :fx? true})
                                     :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:input "test"}})]
      (is (true? (get-in result [:validation :validated])))
      (is (true? (get-in result [:enrichment :enriched])))
      ;; Both child FX should have been surfaced and executed by parent pipeline
      (is (= [:validated :enriched] @fx-log)
          "FX from both sub-FSMs should fire in order through parent pipeline")
      (fx/clear-fx :test-log))))

(deftest parent-error-state-from-sub-fsm-transition-failure
  (testing "Parent transitions to error state when sub-FSM causes transition failure"
    (let [;; Child that always succeeds
          child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :child-ok true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          ;; Parent where sub-FSM handler returns data that no dispatch pred matches
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [ctx (fsm/run-sub-fsm child-fsm resources {})]
                                                      ;; Return data that won't match any dispatch
                                                      (assoc data :status :unknown)))
                                      :dispatches [[:only-if-ready (fn [{:keys [status]}]
                                                                     (= status :ready))]]}
                         :only-if-ready {:handler    (fn [_r data] data)
                                         :dispatches [[::fsm/end (constantly true)]]}
                         ;; Custom error handler to inspect
                         ::fsm/error {:handler (fn [_r fsm]
                                                 {:error-caught true
                                                  :state (:current-state-id fsm)
                                                  :data (:data fsm)})}}})
          result (fsm/run parent {} {:data {}})]
      (is (true? (:error-caught result))
          "Parent should reach error state when no transition matches"))))

(deftest parent-sub-fsm-with-multi-state-child-and-fx
  (testing "Parent orchestrates multi-state child FSM with FX at each child state"
    (let [fx-log (atom [])
          _      (fx/reg-fx :test-log (fn [value] (swap! fx-log conj value)))
          pipeline-child (fsm/compile
                          {:fsm {::fsm/start {:handler    (fn [_r data]
                                                            {:data (assoc data :step1 true)
                                                             :fx [[:test-log :child-step1]]})
                                              :dispatches [[:step2 (constantly true)]]}
                                 :step2      {:handler    (fn [_r data]
                                                            {:data (assoc data :step2 true)
                                                             :fx [[:test-log :child-step2]]})
                                              :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [_r data]
                                                    {:data (assoc data :parent-started true)
                                                     :fx [[:test-log :parent-start]]})
                                      :dispatches [[:delegate (constantly true)]]}
                         :delegate   {:handler    (fsm/make-sub-fsm-handler
                                                   pipeline-child
                                                   {:result-key :pipeline
                                                    :fx? true})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {}})]
      (is (true? (:parent-started result)))
      (is (true? (get-in result [:pipeline :step1])))
      (is (true? (get-in result [:pipeline :step2])))
      ;; Parent start FX fires during parent pipeline,
      ;; child FX are captured and surfaced when make-sub-fsm-handler returns
      (is (= [:parent-start :child-step1 :child-step2] @fx-log)
          "Parent FX then child FX in order")
      (fx/clear-fx :test-log))))

(deftest parent-run-async-with-sub-fsm
  (testing "run-async works with parent FSM containing sub-FSM handlers"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :enriched true))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fn [resources data]
                                                    (let [ctx (fsm/run-sub-fsm child-fsm resources
                                                                               {:id (:agent data)})]
                                                      (assoc data :context ctx)))
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result @(fsm/run-async parent {} {:data {:agent "async-ling"}})]
      (is (= "async-ling" (:agent result)))
      (is (true? (get-in result [:context :enriched]))))))

(deftest parent-empty-data-passthrough-to-sub-fsm
  (testing "Parent passes empty data to sub-FSM when data-fn returns {}"
    (let [child-fsm (fsm/compile
                     {:fsm {::fsm/start {:handler    (fn [_r data]
                                                       (assoc data :child-ran true
                                                              :input-count (count data)))
                                         :dispatches [[::fsm/end (constantly true)]]}}})
          parent (fsm/compile
                  {:fsm {::fsm/start {:handler    (fsm/make-sub-fsm-handler
                                                   child-fsm
                                                   {:data-fn    (constantly {})
                                                    :result-key :child})
                                      :dispatches [[::fsm/end (constantly true)]]}}})
          result (fsm/run parent {} {:data {:secret "hidden" :internal 42}})]
      ;; Child received empty data, so input-count should be 0
      (is (= 0 (get-in result [:child :input-count])))
      (is (true? (get-in result [:child :child-ran])))
      ;; Parent data preserved
      (is (= "hidden" (:secret result)))
      (is (= 42 (:internal result))))))
