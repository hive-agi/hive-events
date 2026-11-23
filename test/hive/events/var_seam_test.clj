(ns hive.events.var-seam-test
  "The facade `hive.events` re-exports ~44 functions from the sub-namespaces.

   A `(def x sub/x)` re-export stores the fn OBJECT. Rebinding `sub/x` (a
   reload, a fix applied in a live image) then leaves the facade holding the
   old fn, so every caller entering through `hive.events` keeps running old
   code while a REPL check through `sub/x` shows the fix working -- the
   'restart to pick up the fix' failure. See epic 20260821211613-0c0e618f.

   The fix is `(defn x [& args] (apply sub/x args))`, which resolves the var
   at call time. This namespace is the invariant gate: it rebinds a leaf and
   asserts the change is visible through the facade with no reload, and it
   fails if a fn-valued frozen alias ever comes back."
  (:require [clojure.java.io]
            [clojure.test :refer [deftest is testing]]
            [hive.events :as ev]
            [hive.events.interceptor :as interceptor]
            [hive.events.router :as router]))

;; =============================================================================
;; The reload property, measured through an alias
;; =============================================================================

(defn- with-rebound-leaf
  "Call `f` with `leaf-var` rebound to `replacement`, restoring the original
   root afterwards. `alter-var-root` and not `with-redefs`: the point is that
   the rebinding survives into code that resolves the var LATER, which is what
   a hot reload does."
  [leaf-var replacement f]
  (let [original @leaf-var]
    (try
      (alter-var-root leaf-var (constantly replacement))
      (f)
      (finally (alter-var-root leaf-var (constantly original))))))

(deftest alias-sees-a-rebound-leaf-test
  (testing "a reload of hive.events.interceptor/enqueue is visible through hive.events/enqueue"
    (with-rebound-leaf #'interceptor/enqueue
                       (fn [& _] ::rebound)
                       (fn []
                         (is (= ::rebound (ev/enqueue [{:id :only}]))
                             "the facade resolved the leaf var at call time")))))

(deftest frozen-copy-does-not-see-a-rebound-leaf-test
  (testing "the CONTROL: the pre-fix shape (def x leaf/x) is frozen"
    ;; `frozen` is what a `(def enqueue interceptor/enqueue)` captured: the fn
    ;; OBJECT of that moment. It is not a var, so rebinding the leaf cannot
    ;; reach it -- it keeps calling the ORIGINAL 2-arity enqueue and rejects
    ;; the 1-arity call the rebinding would have accepted.
    (let [frozen interceptor/enqueue]
      (with-rebound-leaf #'interceptor/enqueue
                         (fn [& _] ::rebound)
                         (fn []
                           (is (= ::rebound (ev/enqueue [{:id :only}]))
                               "the facade followed the var")
                           (is (thrown-with-msg?
                                clojure.lang.ArityException
                                #"hive\.events\.interceptor/enqueue"
                                (frozen [{:id :only}]))
                               (str "the frozen copy still reaches the OLD fn: this "
                                    "is exactly the staleness the var seam removes")))))))

;; =============================================================================
;; Invariant census: no fn-valued frozen alias may exist in the facade
;; =============================================================================

(def ^:private frozen-alias-pattern
  "A top-level `(def name owner/sym)`: the VALUE is a bare qualified symbol,
   i.e. a captured object. Deliberately excludes `(def x (expr))` and
   `(defn ...)`."
  #"(?m)^\(def\s+([^\s\)]+)\s+([a-zA-Z][\w.\-]*\/[^\s\)]+)\s*\)")

(defn- facade-source []
  (slurp (clojure.java.io/resource "hive/events.cljc")))

(deftest no-fn-valued-frozen-alias-test
  (let [src      (facade-source)
        captures (map (fn [[_ n v]] [(symbol n) v])
                      (re-seq frozen-alias-pattern src))]
    (is (seq captures) "the facade was found and parsed (non-vacuity)")
    (let [offenders (->> captures
                         (keep (fn [[n _]]
                                 (when (fn? (var-get (ns-resolve 'hive.events n))) n)))
                         sort)]
      (is (= [] offenders)
          (str "these facade entries capture a fn object instead of delegating "
               "through the var: " offenders)))))

(deftest facade-entries-are-callable-test
  (testing "every re-exported fn is a var holding an IFn"
    (doseq [sym '[->interceptor enqueue execute reg-fx reg-event-fx dispatch
                  dispatch-sync get-event fsm-compile fsm-run fsm-step
                  set-log-fn! stale-registrations]]
      (let [v (ns-resolve 'hive.events sym)]
        (is (var? v) (str sym " is a var"))
        (is (ifn? @v) (str sym " holds an IFn"))))))

(deftest registry-facade-delegates-test
  (testing "the registry trio reaches the router's ONE store rather than a copy"
    (try
      (router/reg-event-fx ::probe (fn [_ _] {}))
      (is (true? (ev/event-registered? ::probe)))
      (is (some? (ev/get-event ::probe)))
      (is (contains? (ev/registered-event-ids) ::probe)
          "the id is visible in the router's registry, not a private copy")
      (finally (router/unreg-event ::probe)))))
