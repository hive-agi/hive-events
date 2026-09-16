(ns hive.events.staleness
  "Which registered handlers a reload has left behind.

   A registration captures a function VALUE. `(reg-fx :k handle-k)` puts the fn
   OBJECT in the registry, not the var. Reload the namespace and `#'handle-k`
   gets a new root binding while the registry keeps the old closure, so the
   image runs code that is no longer on disk.

   Every other signal reports success: the namespace appears in the reload's
   `:loaded` list, the var carries the new arglists, the addon remounts green.
   The only observable difference is IDENTITY, which is what this namespace
   measures. `stale-entries` compares each registered function against the
   current root value of the var it was compiled from and names the ones that
   no longer match.

   Two shapes are invisible here, both on purpose:

   - An ANONYMOUS handler, `(reg-fx :k (fn [v] ...))`, has no var to compare
     against. It is also the shape a reload fixes by itself, because the
     top-level form that registers it re-runs.
   - A handler registered AS A VAR, `(reg-fx :k #'handle-k)`, resolves through
     the var on every call and so cannot go stale.

   What is left is the defect: a registration that names a var and is guarded
   so it runs once, under a `defonce` or behind an `initialized?` flag. clj-reload
   preserves both, so the guarded registration never re-runs.

   JVM only. The mapping from a function back to its var is read off the
   compiled class name, which ClojureScript does not carry; there the scan
   answers with an empty result rather than a wrong one."
  (:require [clojure.string :as str]
            [hive.events.cofx :as cofx]
            [hive.events.fx :as fx]
            [hive.events.router :as router]))

(defn registry-fn
  "The function a registry entry actually invokes, or nil when the entry holds
   nothing this namespace can judge.

   The fx and cofx registries store the handler directly; the event registry
   stores it under :handler beside its interceptor chain. A var entry answers
   nil on purpose: it resolves at call time, so it is never stale."
  [entry]
  (cond
    (var? entry)                    nil
    (fn? entry)                     entry
    (and (map? entry)
         (fn? (:handler entry)))    (:handler entry)
    :else                           nil))

(defn owner-symbol
  "The qualified name of the var `f` was compiled from, or nil when it did not
   come from one.

   Read off the class name, which the compiler munges from the defining
   namespace and name. A fn that is not top-level carries extra segments or a
   numeric suffix, and demunges to something with the wrong shape; that is the
   signal that there is no var to compare against."
  [f]
  #?(:clj
     (when (ifn? f)
       (let [demunged (clojure.lang.Compiler/demunge (.getName (class f)))
             parts    (str/split demunged #"/")]
         (when (and (= 2 (count parts))
                    (not (str/includes? (second parts) "--")))
           (symbol (first parts) (second parts)))))
     :cljs nil))

(defn owner-var
  "The live var `f` was compiled from, looked up in the namespace as it exists
   NOW. Deliberately not the var object captured at registration time: a reload
   may have replaced the namespace, and the question is what the code on disk
   defines today."
  [f]
  #?(:clj
     (when-let [sym (owner-symbol f)]
       (some-> (find-ns (symbol (namespace sym)))
               (ns-interns)
               (get (symbol (name sym)))))
     :cljs nil))

(defn current-registries
  "The registries hive.events owns, keyed by the name this namespace reports
   them under."
  []
  {:fx    (fx/registry-snapshot)
   :cofx  (cofx/registry-snapshot)
   :event (router/registry-snapshot)})

(defn stale-entries
  "Registered handlers whose function is no longer what their var holds.

   Options:
   - :registries  {registry-id {key entry}} to judge instead of the live ones.
     Pass it and the scan is pure, which is how the tests drive it.
   - :namespaces  only report handlers owned by these namespaces (symbols or
     strings). This is what makes the scan useful after a reload: ask only
     about what was reloaded.

   Returns a vector of {:registry :id :owner :var}, one per stale entry."
  ([] (stale-entries {}))
  ([{:keys [registries namespaces]}]
   (let [regs  (or registries (current-registries))
         wanted (when (seq namespaces) (set (map symbol namespaces)))]
     (vec
      (for [[registry entries] regs
            [id entry]         entries
            :let  [f    (registry-fn entry)
                   avar (some-> f owner-var)]
            :when (and avar
                       (let [owner-ns (ns-name (:ns (meta avar)))]
                         (and (or (nil? wanted) (contains? wanted owner-ns))
                              (not (identical? f @avar)))))]
        {:registry registry
         :id       id
         :owner    (symbol (str (ns-name (:ns (meta avar))))
                           (str (:name (meta avar))))
         :var      avar})))))
