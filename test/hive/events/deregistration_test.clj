(ns hive.events.deregistration-test
  "Tests for E1: Deregistration API at the hive-events library level.

   Validates unreg-fx, unreg-cofx, registered-fx-ids, and
   registered-cofx-ids functions in hive.events.fx and hive.events.cofx."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive.events.fx :as fx]
            [hive.events.cofx :as cofx]))

;; =============================================================================
;; Fixture: Save/restore registries
;; =============================================================================

(defn clean-registry-fixture [f]
  (let [old-fx @(var-get #'fx/fx-registry)
        old-cofx @(var-get #'cofx/cofx-registry)]
    (try
      (reset! (var-get #'fx/fx-registry) {})
      (reset! (var-get #'cofx/cofx-registry) {})
      (f)
      (finally
        (reset! (var-get #'fx/fx-registry) old-fx)
        (reset! (var-get #'cofx/cofx-registry) old-cofx)))))

(use-fixtures :each clean-registry-fixture)

;; =============================================================================
;; fx/unreg-fx
;; =============================================================================

(deftest unreg-fx-removes-handler
  (testing "unreg-fx removes a registered handler and returns true"
    (fx/reg-fx :test/my-fx (fn [_] nil))
    (is (fn? (fx/get-fx :test/my-fx)))
    (is (true? (fx/unreg-fx :test/my-fx)))
    (is (nil? (fx/get-fx :test/my-fx)))))

(deftest unreg-fx-returns-false-when-not-found
  (testing "unreg-fx returns false for non-existent handler"
    (is (false? (fx/unreg-fx :test/nonexistent)))))

(deftest unreg-fx-idempotent
  (testing "Second unreg-fx call returns false"
    (fx/reg-fx :test/once (fn [_] nil))
    (is (true? (fx/unreg-fx :test/once)))
    (is (false? (fx/unreg-fx :test/once)))))

;; =============================================================================
;; fx/registered-fx-ids
;; =============================================================================

(deftest registered-fx-ids-returns-set
  (testing "registered-fx-ids returns set of registered IDs"
    (is (= #{} (fx/registered-fx-ids))
        "Empty registry should return empty set")
    (fx/reg-fx :test/alpha (fn [_] nil))
    (fx/reg-fx :test/beta (fn [_] nil))
    (is (= #{:test/alpha :test/beta} (fx/registered-fx-ids)))))

(deftest registered-fx-ids-reflects-unreg
  (testing "registered-fx-ids updates after unreg-fx"
    (fx/reg-fx :test/stay (fn [_] nil))
    (fx/reg-fx :test/leave (fn [_] nil))
    (fx/unreg-fx :test/leave)
    (is (= #{:test/stay} (fx/registered-fx-ids)))))

;; =============================================================================
;; cofx/unreg-cofx
;; =============================================================================

(deftest unreg-cofx-removes-handler
  (testing "unreg-cofx removes a registered handler and returns true"
    (cofx/reg-cofx :test/my-cofx (fn [c] c))
    (is (fn? (cofx/get-cofx :test/my-cofx)))
    (is (true? (cofx/unreg-cofx :test/my-cofx)))
    (is (nil? (cofx/get-cofx :test/my-cofx)))))

(deftest unreg-cofx-returns-false-when-not-found
  (testing "unreg-cofx returns false for non-existent handler"
    (is (false? (cofx/unreg-cofx :test/nonexistent)))))

(deftest unreg-cofx-idempotent
  (testing "Second unreg-cofx call returns false"
    (cofx/reg-cofx :test/once (fn [c] c))
    (is (true? (cofx/unreg-cofx :test/once)))
    (is (false? (cofx/unreg-cofx :test/once)))))

;; =============================================================================
;; cofx/registered-cofx-ids
;; =============================================================================

(deftest registered-cofx-ids-returns-set
  (testing "registered-cofx-ids returns set of registered IDs"
    (is (= #{} (cofx/registered-cofx-ids))
        "Empty registry should return empty set")
    (cofx/reg-cofx :test/alpha (fn [c] c))
    (cofx/reg-cofx :test/beta (fn [c] c))
    (is (= #{:test/alpha :test/beta} (cofx/registered-cofx-ids)))))

(deftest registered-cofx-ids-reflects-unreg
  (testing "registered-cofx-ids updates after unreg-cofx"
    (cofx/reg-cofx :test/stay (fn [c] c))
    (cofx/reg-cofx :test/leave (fn [c] c))
    (cofx/unreg-cofx :test/leave)
    (is (= #{:test/stay} (cofx/registered-cofx-ids)))))

;; =============================================================================
;; Thread-safety
;; =============================================================================

(deftest unreg-fx-thread-safety
  (testing "Concurrent unreg-fx does not throw or corrupt state"
    (let [ids (mapv #(keyword "test" (str "fx-" %)) (range 50))]
      (doseq [id ids]
        (fx/reg-fx id (fn [_] nil)))
      (let [futures (mapv (fn [id] (future (fx/unreg-fx id))) ids)
            results (mapv deref futures)]
        (is (every? true? results))
        (is (= #{} (fx/registered-fx-ids)))))))

(deftest unreg-cofx-thread-safety
  (testing "Concurrent unreg-cofx does not throw or corrupt state"
    (let [ids (mapv #(keyword "test" (str "cofx-" %)) (range 50))]
      (doseq [id ids]
        (cofx/reg-cofx id (fn [c] c)))
      (let [futures (mapv (fn [id] (future (cofx/unreg-cofx id))) ids)
            results (mapv deref futures)]
        (is (every? true? results))
        (is (= #{} (cofx/registered-cofx-ids)))))))
