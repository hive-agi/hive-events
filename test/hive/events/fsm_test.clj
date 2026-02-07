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
