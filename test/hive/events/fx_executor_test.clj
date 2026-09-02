(ns hive.events.fx-executor-test
  "The effect-invocation seam.

   An embedder (hive-mcp) wraps effect execution with metrics and a loud-fail
   counter for unregistered effects. Before this seam existed, the only way to
   get that was a SECOND do-fx, which is how the host ended up with its own copy
   of the whole dispatch path: delegating to the library would have silently
   dropped both.

   The claims: the default is unchanged when nothing is installed, an installed
   policy sees EVERY effect (`:db` included, which do-fx used to route around),
   ordering still puts `:db` first, and clearing restores the default."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive.events.fx :as fx]))

(defn- isolate-registry [f]
  (let [snapshot (fx/registry-snapshot)]
    (try (f)
         (finally
           (fx/restore-registry! snapshot)
           (fx/clear-fx-executor!)))))

(use-fixtures :each isolate-registry)

;; =============================================================================
;; Default policy
;; =============================================================================

(deftest default-policy-runs-handlers
  (testing "with nothing installed, effects reach their handlers"
    (let [seen (atom [])]
      (fx/reg-fx ::a (fn [v] (swap! seen conj [::a v])))
      (fx/do-fx {::a 1})
      (is (= [[::a 1]] @seen))
      (is (false? (fx/fx-executor-installed?))))))

(deftest default-policy-does-not-throw-on-a-missing-handler
  (testing "an unregistered effect is warned about, not thrown"
    (is (nil? (fx/do-fx {::never-registered 1})))))

(deftest default-policy-does-not-throw-when-a-handler-throws
  (testing "do-fx runs after the chain committed, so it must not throw"
    (fx/reg-fx ::boom (fn [_] (throw (ex-info "boom" {}))))
    (is (nil? (fx/do-fx {::boom 1})))))

;; =============================================================================
;; Installed policy
;; =============================================================================

(deftest installed-policy-sees-every-effect
  (testing "including :db, which do-fx used to invoke directly"
    (let [seen (atom [])]
      (fx/set-fx-executor! (fn [id v] (swap! seen conj [id v])))
      (fx/do-fx {:db 0 ::a 1 ::b 2})
      (is (= #{[:db 0] [::a 1] [::b 2]} (set @seen))
          ":db must not bypass the executor, or an embedder's metrics undercount")
      (is (= [:db 0] (first @seen))
          ":db still runs first"))))

(deftest installed-policy-replaces-the-default
  (testing "the registered handler is not called unless the policy calls it"
    (let [handler-ran (atom false)
          observed    (atom [])]
      (fx/reg-fx ::a (fn [_] (reset! handler-ran true)))
      (fx/set-fx-executor! (fn [id v] (swap! observed conj [id v])))
      (fx/do-fx {::a 1})
      (is (= [[::a 1]] @observed))
      (is (false? @handler-ran)
          "the policy owns lookup and invocation; it did not delegate here"))))

(deftest a-policy-can-observe-and-delegate
  (testing "the wrap-and-delegate shape an embedder actually uses"
    (let [counted (atom 0)
          ran     (atom [])]
      (fx/reg-fx ::a (fn [v] (swap! ran conj v)))
      (fx/set-fx-executor! (fn [id v]
                             (swap! counted inc)
                             (fx/default-invoke-fx-handler id v)))
      (fx/do-fx {::a 1 ::b 2})
      (is (= 2 @counted) "the policy saw both effects")
      (is (= [1] @ran) "and the registered handler still ran"))))

(deftest installed-policy-covers-do-fx-seq
  (testing "the sequential path shares the seam, and keeps order and repeats"
    (let [seen (atom [])]
      (fx/set-fx-executor! (fn [id v] (swap! seen conj [id v])))
      (fx/do-fx-seq [[::log 1] [::http 2] [::log 3]])
      (is (= [[::log 1] [::http 2] [::log 3]] @seen)))))

(deftest fx-interceptor-still-wins-over-the-executor
  (testing "sub-FSM effect capture is unchanged by the seam"
    (let [captured (atom nil)
          executor (atom 0)]
      (fx/set-fx-executor! (fn [_ _] (swap! executor inc)))
      (binding [fx/*fx-interceptor* (fn [effects] (reset! captured effects))]
        (fx/do-fx-seq [[::a 1]]))
      (is (= [[::a 1]] @captured))
      (is (zero? @executor)
          "a captured sub-FSM effect must not be executed, nor counted"))))

;; =============================================================================
;; Lifecycle
;; =============================================================================

(deftest clearing-restores-the-default
  (let [ran (atom false)]
    (fx/reg-fx ::a (fn [_] (reset! ran true)))
    (fx/set-fx-executor! (fn [_ _] nil))
    (fx/do-fx {::a 1})
    (is (false? @ran))
    (fx/clear-fx-executor!)
    (is (false? (fx/fx-executor-installed?)))
    (fx/do-fx {::a 1})
    (is (true? @ran) "the default policy is back")))

(deftest set-returns-the-policy-and-last-writer-wins
  (let [f (fn [_ _] nil)
        seen (atom nil)]
    (is (identical? f (fx/set-fx-executor! f)))
    (fx/set-fx-executor! (fn [id _] (reset! seen id)))
    (fx/do-fx {::a 1})
    (is (= ::a @seen))))
