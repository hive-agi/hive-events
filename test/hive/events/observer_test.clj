(ns hive.events.observer-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive.events.observer :as obs]
            [hive.events.router :as router]))

(use-fixtures :each (fn [t] (obs/clear-observers!) (t) (obs/clear-observers!)))

;; =============================================================================
;; Selection (pure)
;; =============================================================================

(deftest select-observers-filters-by-event-id
  (let [a (obs/on-events ::a #{:evt/one} identity)
        b (obs/on-events ::b #{:evt/two} identity)
        g (obs/on-any-event ::g identity)
        m {::a a ::b b ::g g}]
    (testing "targeted observers see only their own event ids"
      (is (= [::a ::g] (mapv obs/observer-id (obs/select-observers m :evt/one))))
      (is (= [::b ::g] (mapv obs/observer-id (obs/select-observers m :evt/two)))))
    (testing "an unobserved event selects only the global observer"
      (is (= [::g] (mapv obs/observer-id (obs/select-observers m :evt/other)))))
    (testing "selection is deterministic, ordered by observer-id"
      (is (= (obs/select-observers m :evt/one)
             (obs/select-observers (into {} (reverse m)) :evt/one))))))

;; =============================================================================
;; Registry
;; =============================================================================

(deftest register-is-idempotent-by-id
  (obs/register-observer! (obs/on-events ::dup #{:evt/x} identity))
  (obs/register-observer! (obs/on-events ::dup #{:evt/y} identity))
  (is (= 1 (count (obs/observers))) "same id replaces, never accumulates")
  (is (empty? (obs/select-observers (obs/observers) :evt/x)))
  (is (seq (obs/select-observers (obs/observers) :evt/y))))

(deftest remove-observer-deregisters
  (obs/register-observer! (obs/on-any-event ::gone identity))
  (obs/remove-observer! ::gone)
  (is (zero? (obs/notify! :evt/anything {}))))

;; =============================================================================
;; Isolation contract — a broken observer must not affect anything else
;; =============================================================================

(deftest observer-throw-is-contained
  (let [reached (atom [])]
    (obs/register-observer!
     (obs/on-any-event ::a-boom (fn [_] (throw (ex-info "boom" {})))))
    (obs/register-observer!
     (obs/on-any-event ::b-ok (fn [_] (swap! reached conj :b))))
    (is (= 2 (obs/notify! :evt/any {})) "both observers are attempted")
    (is (= [:b] @reached) "a sibling still runs after one throws")))

;; =============================================================================
;; OCP — a foreign implementation needs no change to this namespace
;; =============================================================================

(defrecord PrefixObserver [id prefix sink]
  obs/IEventObserver
  (observer-id [_] id)
  (observes? [_ event-id] (= prefix (namespace event-id)))
  (on-event [_ context] (swap! sink conj (-> context :coeffects :event first))))

(deftest custom-observer-implementation-participates
  (let [sink (atom [])]
    (obs/register-observer! (->PrefixObserver ::pre "carto" sink))
    (is (= 1 (obs/notify! :carto/scan-finished
                          {:coeffects {:event [:carto/scan-finished]}})))
    (is (zero? (obs/notify! :other/event {:coeffects {:event [:other/event]}})))
    (is (= [:carto/scan-finished] @sink))))

;; =============================================================================
;; Router integration — observers do not displace the handler
;; =============================================================================

(deftest handler-and-observer-both-run
  (let [trace (atom [])]
    (router/clear-event :test/evt)
    (router/reg-event-fx :test/evt (fn [_ _] (swap! trace conj :handler) {}))
    (obs/register-observer!
     (obs/on-events ::watch #{:test/evt} (fn [_] (swap! trace conj :observer))))
    (router/dispatch-sync [:test/evt])
    (is (= [:handler :observer] @trace)
        "handler runs first, observer sees the post-effect context")
    (router/clear-event :test/evt)))

(deftest observer-fires-for-unhandled-event
  (let [seen (atom 0)]
    (obs/register-observer!
     (obs/on-events ::orphan #{:test/no-handler} (fn [_] (swap! seen inc))))
    (router/dispatch-sync [:test/no-handler])
    (is (= 1 @seen) "an event with no handler still reaches observers")))
