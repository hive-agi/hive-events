(ns hive.events.log
  "Minimal pluggable logging for hive-events.

   Three levels: :debug, :warn, :error

   Default behavior:
   - JVM: prints to *err* (stderr)
   - JS: uses console.log/warn/error

   Override:
     (set-log-fn! (fn [level msg & args]
                    ;; your logging here
                    ))

   Disable:
     (set-log-fn! (fn [& _]))

   Restore default:
     (set-log-fn! nil)")

(defonce ^:private custom-log-fn (atom nil))

(defn set-log-fn!
  "Set a custom log function. Receives (f level msg & args).
   level is :debug, :warn, or :error.
   Pass nil to restore default behavior."
  [f]
  (reset! custom-log-fn f))

(defn- default-log [level msg args]
  (let [prefix (str "[hive.events:" (name level) "]")]
    #?(:clj  (binding [*out* *err*]
               (apply println prefix msg args))
       :cljs (case level
               :error (apply js/console.error prefix msg args)
               :warn  (apply js/console.warn prefix msg args)
               (apply js/console.log prefix msg args)))))

(defn log
  "Log a message at the given level (:debug, :warn, :error)."
  [level msg & args]
  (if-let [f @custom-log-fn]
    (apply f level msg args)
    (default-log level msg args)))

(defn debug
  "Log at :debug level."
  [msg & args]
  (apply log :debug msg args))

(defn warn
  "Log at :warn level."
  [msg & args]
  (apply log :warn msg args))

(defn error
  "Log at :error level."
  [msg & args]
  (apply log :error msg args))
