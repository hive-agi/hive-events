(ns hive.events.registry-test
  "The event registry is ONE store. A host that wraps dispatch with its own
   validation or telemetry must read and write THIS registry; a second atom
   with the same API makes a handler registered by a library consumer
   invisible to the host's dispatch, and the miss is silent."
  (:require [clojure.java.io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive.events]
            [hive.events.fx :as fx]
            [hive.events.interceptor :as interceptor]
            [hive.events.router :as router]))

(defn- with-clean-registry [f]
  (let [saved (router/registry-snapshot)]
    (try
      (router/restore-registry! {})
      (f)
      (finally (router/restore-registry! saved)))))

(use-fixtures :each with-clean-registry)

;; =============================================================================
;; Entry shape
;; =============================================================================

(deftest stored-entry-is-the-user-chain-plus-the-handler-test
  (let [ic (interceptor/->interceptor :id :mine :before identity)]
    (router/reg-event-fx ::e [ic] (fn [_ _] {}))
    (let [entry (router/get-event ::e)]
      (is (= [:mine] (mapv :id (:interceptors entry)))
          "the stored chain is the caller's chain, normalized")
      (is (fn? (:handler entry))))))

(deftest registration-inspection-test
  (router/reg-event-fx ::a (fn [_ _] {}))
  (router/reg-event-fx ::b (fn [_ _] {}))
  (is (true? (router/handler-registered? ::a)))
  (is (false? (router/handler-registered? ::nope)))
  (is (= #{::a ::b} (router/registered-event-ids)))
  (is (some? (router/get-event ::a)))
  (is (nil? (router/get-event ::nope))))

(deftest unreg-event-reports-whether-it-removed-test
  (router/reg-event-fx ::a (fn [_ _] {}))
  (is (true? (router/unreg-event ::a)))
  (is (false? (router/unreg-event ::a)))
  (is (false? (router/handler-registered? ::a))))

;; =============================================================================
;; append-interceptor!
;; =============================================================================

(deftest append-interceptor-is-idempotent-and-guarded-test
  (let [ic (interceptor/->interceptor :id :extra :before identity)]
    (testing "an unregistered event cannot be decorated"
      (is (false? (router/append-interceptor! ::missing ic))))
    (router/reg-event-fx ::e (fn [_ _] {}))
    (is (true? (router/append-interceptor! ::e ic)))
    (is (false? (router/append-interceptor! ::e ic)) "same :id appends once")
    (is (= [:extra] (mapv :id (:interceptors (router/get-event ::e)))))))

;; =============================================================================
;; Dispatch honours the stored chain and runs the handler exactly once
;; =============================================================================

(deftest dispatch-runs-appended-interceptor-then-handler-once-test
  (let [calls (atom [])
        ic    (interceptor/->interceptor
               :id :count
               :before (fn [ctx] (swap! calls conj :interceptor) ctx))]
    (fx/reg-fx ::sink (fn [v] (swap! calls conj [:fx v])))
    (try
      (router/reg-event-fx ::e (fn [_ ev] (swap! calls conj :handler) {::sink (second ev)}))
      (router/append-interceptor! ::e ic)
      (let [ctx (router/dispatch-sync [::e 42])]
        (is (= 42 (get-in ctx [:effects ::sink])))
        (is (= [:interceptor :handler [:fx 42]] @calls)
            "the handler runs once, after the appended interceptor"))
      (finally (fx/unreg-fx ::sink)))))

(deftest handler-effects-merge-into-existing-effects-test
  (let [seed (interceptor/->interceptor
              :id :seed
              :before (fn [ctx] (assoc ctx :effects {::pre 1})))]
    (router/reg-event-fx ::e [seed] (fn [_ _] {::post 2}))
    (let [ctx (router/dispatch-sync [::e])]
      (is (= {::pre 1 ::post 2} (:effects ctx))
          "the handler merges rather than replacing what the chain already set"))))

;; =============================================================================
;; Snapshot / restore
;; =============================================================================

;; =============================================================================
;; Facade hygiene
;;
;; hive.events is a wall of `(def x other/x)`. A name def'd twice there compiles
;; without a warning and the LAST one wins, so a re-export can be silently
;; replaced by an unrelated subsystem's function of the same name.
;; =============================================================================

(deftest facade-defs-no-name-twice-test
  (let [src   (slurp (clojure.java.io/resource "hive/events.cljc"))
        names (->> (re-seq #"(?m)^\(def\s+([^\s\)]+)" src)
                   (map second))
        dupes (->> (frequencies names)
                   (filter (fn [[_ n]] (> n 1)))
                   (map first)
                   set)]
    (is (seq names) "the facade was found and parsed")
    (is (= #{} dupes)
        (str "these names are def'd more than once in hive.events: " dupes))))

(deftest facade-registry-exports-point-at-the-router-test
  (is (identical? (var-get #'hive.events/event-registered?) router/handler-registered?))
  (is (identical? (var-get #'hive.events/get-event) router/get-event))
  (is (identical? (var-get #'hive.events/unreg-event) router/unreg-event)))

(deftest snapshot-restore-round-trips-test
  (router/reg-event-fx ::a (fn [_ _] {}))
  (let [snap (router/registry-snapshot)]
    (router/clear-event)
    (is (= #{} (router/registered-event-ids)))
    (router/restore-registry! snap)
    (is (= #{::a} (router/registered-event-ids)))))
