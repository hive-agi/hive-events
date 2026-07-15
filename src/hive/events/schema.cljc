(ns hive.events.schema
  "Schema-driven event registration — the hive-events leg of the malli macro
   layer. `reg-event-schema` registers an fx handler whose EVENT PAYLOAD (the
   second element of the [id payload] event vector) is coerced + validated against
   ONE registered malli schema-key by a :before interceptor, before the handler
   runs — declare the event's payload schema once, its guard is wired.

   hive-events is cljc/cljs-safe and malli's derive lever is clj-only, so
   compile-op is resolved LAZILY via requiring-resolve (guarded). When it is
   unresolvable — cljs, or hive-spi absent — the interceptor is a pass-through, so
   registration never fails and the module stays platform-safe."
  (:require [hive.events.router :as router]
            [hive.events.interceptor :as interceptor]))

(defn- resolve-compile-op
  "The clj-only hive-spi.schema.derive/compile-op, or nil (cljs, or hive-spi
   absent). requiring-resolve THROWS on an absent ns, so it is guarded."
  []
  #?(:clj  (try (requiring-resolve 'hive-spi.schema.derive/compile-op)
                (catch Throwable _ nil))
     :cljs nil))

(defn coerce-event-interceptor
  "A :before interceptor that coerces + validates the event PAYLOAD (second
   element of the [id payload] event vector) against `schema-key`, assoc'ing the
   coerced payload back into the event. An invalid payload throws ex-info
   {:error :schema/invalid ...} before the handler runs. When compile-op is
   unresolvable (cljs / hive-spi absent) it is a pass-through — validation is
   best-effort, never a load or dispatch blocker."
  [schema-key]
  (interceptor/->interceptor
   :id :schema-coerce
   :before (fn [ctx]
             (if-let [compile-op (resolve-compile-op)]
               (let [{:keys [coerce]} (compile-op schema-key)
                     event   (get-in ctx [:coeffects :event])
                     coerced (coerce (second event))]
                 (assoc-in ctx [:coeffects :event] (assoc event 1 coerced)))
               ctx))))

(defn reg-event-schema
  "Register an fx event handler whose payload is coerced + validated against a
   registered malli `schema-key`. Prepends a schema-coerce :before interceptor to
   `interceptors` and delegates to hive.events.router/reg-event-fx. The handler
   signature is unchanged — (fn [coeffects event] effects-map) — but the event's
   payload arrives coerced; an invalid payload throws before the handler runs."
  ([id schema-key handler]
   (reg-event-schema id schema-key [] handler))
  ([id schema-key interceptors handler]
   (router/reg-event-fx id
                        (into [(coerce-event-interceptor schema-key)] interceptors)
                        handler)))
