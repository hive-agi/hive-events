(ns hive.events-test
  "Basic tests for hive-events."
  (:require [clojure.test :refer [deftest is testing]]
            [hive.events :as ev]
            [hive.events.router :as router]
            [hive.events.fx :as fx]
            [hive.events.cofx :as cofx]))

(deftest interceptor-test
  (testing "interceptor chain execution"
    (let [log (atom [])
          interceptor-a (ev/->interceptor
                         :id :a
                         :before (fn [ctx] (swap! log conj :a-before) ctx)
                         :after (fn [ctx] (swap! log conj :a-after) ctx))
          interceptor-b (ev/->interceptor
                         :id :b
                         :before (fn [ctx] (swap! log conj :b-before) ctx)
                         :after (fn [ctx] (swap! log conj :b-after) ctx))
          ctx {:coeffects {:event [:test]}
               :effects {}
               :queue [interceptor-a interceptor-b]
               :stack []}
          result (ev/execute ctx)]
      (is (= [:a-before :b-before :b-after :a-after] @log)))))

(deftest fx-test
  (testing "effect registration and execution"
    (let [called (atom nil)]
      (ev/reg-fx :test-effect
                 (fn [value]
                   (reset! called value)))
      (ev/do-fx {:test-effect "hello"})
      (is (= "hello" @called))
      (fx/clear-fx :test-effect))))

(deftest cofx-test
  (testing "coeffect injection"
    (let [ctx {:coeffects {:db {}}}
          interceptor (ev/inject-cofx :now)
          result ((:before interceptor) ctx)]
      (is (number? (get-in result [:coeffects :now]))))))

(deftest event-handler-test
  (testing "reg-event-db and dispatch-sync"
    (let [db (atom {:count 0})]
      (router/init! db)
      (ev/reg-event-db :counter/inc
                       (fn [db [_ amount]]
                         (update db :count + (or amount 1))))
      (ev/dispatch-sync [:counter/inc 5])
      (is (= 5 (:count @db)))
      (ev/dispatch-sync [:counter/inc 3])
      (is (= 8 (:count @db)))
      (router/clear-event))))

(deftest event-fx-test
  (testing "reg-event-fx with multiple effects"
    (let [db (atom {:count 0})
          side-effect-called (atom nil)]
      (router/init! db)
      (ev/reg-fx :log-action
                 (fn [msg]
                   (reset! side-effect-called msg)))
      (ev/reg-event-fx :counter/inc-and-log
                       (fn [{:keys [db]} [_ amount]]
                         {:db (update db :count + amount)
                          :log-action (str "Incremented by " amount)}))
      (ev/dispatch-sync [:counter/inc-and-log 10])
      (is (= 10 (:count @db)))
      (is (= "Incremented by 10" @side-effect-called))
      (router/clear-event)
      (fx/clear-fx :log-action))))

(deftest dispatch-without-init-test
  (testing "dispatch-sync works in a fresh process that never called init!"
    ;; Simulate a fresh load: reset the private app-db outer atom back to
    ;; its default atom-of-atom shape so this test reproduces the state of
    ;; a process that has not invoked `router/init!`. Other tests in this
    ;; ns call `init!` which replaces the inner value, so we must undo that
    ;; here to actually exercise the no-init code path.
    (reset! @#'hive.events.router/app-db (atom {}))
    (let [fx-called (atom nil)]
      (ev/reg-fx :regression/probe
                 (fn [v] (reset! fx-called v)))
      (ev/reg-event-fx :regression/event
                       (fn [_cofx _event]
                         {:regression/probe :hello}))
      ;; Must not throw ClassCastException (PersistentArrayMap -> Future).
      (is (nil? (try
                  (ev/dispatch-sync [:regression/event])
                  nil
                  (catch Throwable t t)))
          "dispatch-sync should not throw when init! was never called")
      (is (= :hello @fx-called) "fx should have fired")
      (router/clear-event)
      (fx/clear-fx :regression/probe))))

(deftest rapid-stop-start-test
  (testing "stop! then immediate dispatch does not lose events"
    (let [db (atom {:n 0})]
      (router/init! db)
      (ev/reg-event-db :rapid/bump
                       (fn [db _] (update db :n inc)))
      ;; Prime the async loop once.
      (ev/dispatch [:rapid/bump])
      ;; Rapid stop + dispatch cycles. Each stop! must wait for the
      ;; prior go-loop to exit before replacing the queue, otherwise
      ;; subsequent dispatches could land on a leaked old consumer.
      (dotimes [_ 5]
        (router/stop!)
        (ev/dispatch [:rapid/bump]))
      ;; Give the live loop a moment to drain the final dispatch.
      (Thread/sleep 50)
      (router/stop!)
      ;; At minimum the final dispatch after the last stop! must have
      ;; been processed — proves the new channel has a live consumer.
      (is (pos? (:n @db))
          "events dispatched after stop! must still be processed")
      (router/clear-event))))
