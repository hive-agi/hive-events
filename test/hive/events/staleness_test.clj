(ns hive.events.staleness-test
  "The reload-staleness scan.

   Every test here drives `stale-entries` with an EXPLICIT registry map, so the
   scan is exercised as the pure function it is and the live hive.events
   registries are never touched.

   `with-redefs` stands in for the reload: it replaces the var's root binding
   while a previously captured function value stays behind, which is exactly
   the state a defonce-guarded registration is left in after clj-reload has
   loaded the new code."
  (:require [clojure.test :refer [deftest testing is]]
            [hive.events.staleness :as staleness]))

(defn probe-handler
  "Stand-in for a registered effect handler. Its identity is the whole point."
  [v]
  {:before v})

(defn other-handler [v] {:other v})

;; =============================================================================
;; Reading a function back to its var
;; =============================================================================

(deftest owner-symbol-names-the-defining-var
  (testing "a top-level defn is traceable to its qualified name"
    (is (= 'hive.events.staleness-test/probe-handler
           (staleness/owner-symbol probe-handler))))
  (testing "an anonymous fn is not, and that is reported as nil rather than guessed"
    (is (nil? (staleness/owner-symbol (fn [v] v)))))
  (testing "a closure returned by a fn is not traceable either"
    (is (nil? (staleness/owner-symbol ((fn [] (fn [v] v)))))))
  (testing "a non-fn IFn has no owning var"
    (is (nil? (staleness/owner-symbol :some-keyword)))))

(deftest owner-var-resolves-in-the-namespace-as-it-is-now
  (is (= #'probe-handler (staleness/owner-var probe-handler))))

;; =============================================================================
;; What counts as a registry entry
;; =============================================================================

(deftest registry-fn-reads-both-registry-shapes
  (testing "fx and cofx store the handler directly"
    (is (= probe-handler (staleness/registry-fn probe-handler))))
  (testing "the event registry stores it beside its interceptor chain"
    (is (= probe-handler (staleness/registry-fn {:interceptors []
                                                 :handler      probe-handler}))))
  (testing "a var entry is not judged: it resolves at call time and cannot go stale"
    (is (nil? (staleness/registry-fn #'probe-handler))))
  (testing "anything else is not a handler"
    (is (nil? (staleness/registry-fn {:no :handler})))
    (is (nil? (staleness/registry-fn 42)))))

;; =============================================================================
;; The scan
;; =============================================================================

(deftest a-current-registration-is-not-stale
  (is (= [] (staleness/stale-entries
             {:registries {:fx {:probe probe-handler}}}))))

(deftest a-registration-that-outlived-its-var-is-reported
  (let [captured probe-handler]
    (with-redefs [probe-handler (fn [v] {:after v})]
      (let [found (staleness/stale-entries {:registries {:fx {:probe captured}}})]
        (is (= 1 (count found)))
        (is (= {:registry :fx
                :id       :probe
                :owner    'hive.events.staleness-test/probe-handler}
               (dissoc (first found) :var)))
        (is (= #'probe-handler (:var (first found)))
            "the report names the var to re-register, not just the key")))))

(deftest the-event-registry-shape-is-scanned-too
  (let [captured probe-handler]
    (with-redefs [probe-handler (fn [v] {:after v})]
      (is (= [{:registry :event
               :id       :some/event
               :owner    'hive.events.staleness-test/probe-handler}]
             (map #(dissoc % :var)
                  (staleness/stale-entries
                   {:registries {:event {:some/event {:interceptors []
                                                      :handler      captured}}}})))))))

(deftest a-handler-registered-as-a-var-is-immune
  (testing "the var resolves on every call, so a reload rewires it by itself"
    (with-redefs [probe-handler (fn [v] {:after v})]
      (is (= [] (staleness/stale-entries
                 {:registries {:fx {:probe #'probe-handler}}}))))))

(deftest an-anonymous-handler-is-invisible
  (testing "there is no var to compare against, and a reload re-runs the form
            that registered it"
    (let [captured (fn [v] {:anon v})]
      (is (= [] (staleness/stale-entries
                 {:registries {:fx {:probe captured}}}))))))

(deftest the-namespace-filter-narrows-the-scan
  (let [captured-probe probe-handler
        captured-other other-handler]
    (with-redefs [probe-handler (fn [v] {:after v})
                  other-handler (fn [v] {:after v})]
      (let [regs {:registries {:fx {:probe captured-probe
                                    :other captured-other}}}]
        (testing "unfiltered, both stale entries surface"
          (is (= 2 (count (staleness/stale-entries regs)))))
        (testing "a namespace that owns neither answers empty"
          (is (= [] (staleness/stale-entries
                     (assoc regs :namespaces ['no.such.namespace])))))
        (testing "the owning namespace answers with both, which is what a
                  reload report asks for: only what was just reloaded"
          (is (= 2 (count (staleness/stale-entries
                           (assoc regs :namespaces
                                  ['hive.events.staleness-test]))))))
        (testing "strings are accepted the way a reload result spells them"
          (is (= 2 (count (staleness/stale-entries
                           (assoc regs :namespaces
                                  ["hive.events.staleness-test"]))))))))))

(deftest several-registries-are-scanned-in-one-pass
  (let [captured probe-handler]
    (with-redefs [probe-handler (fn [v] {:after v})]
      (is (= #{[:fx :a] [:cofx :b] [:event :c]}
             (set (map (juxt :registry :id)
                       (staleness/stale-entries
                        {:registries {:fx    {:a captured}
                                      :cofx  {:b captured}
                                      :event {:c {:handler captured}}}}))))))))

(deftest the-live-registries-are-readable
  (testing "current-registries names the three hive.events owns"
    (is (= #{:fx :cofx :event} (set (keys (staleness/current-registries)))))))
