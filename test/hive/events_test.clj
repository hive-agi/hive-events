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
