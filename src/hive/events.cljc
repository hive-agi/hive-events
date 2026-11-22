(ns hive.events
  "Unified event system for Clojure/ClojureScript.

   Combines:
   - re-frame interceptor chain (portable to JVM)
   - re-frame effects/coeffects system
   - domino-style declarative models with async support

   Design principles:
   - Events as data
   - Pure handlers returning effect maps
   - Interceptors for cross-cutting concerns
   - Async-first with core.async

   Usage:
     (require '[hive.events :as ev])

     ;; Register effect handlers
     (ev/reg-fx :http (fn [request] ...))
     (ev/reg-fx :db (fn [tx] ...))

     ;; Register event handlers
     (ev/reg-event-fx :user/login
       [ev/debug ev/validate]  ; interceptors
       (fn [{:keys [db]} [_ credentials]]
         {:db (assoc db :loading? true)
          :http {:method :post :url \"/login\" :body credentials}}))

     ;; Dispatch events
     (ev/dispatch [:user/login {:email \"...\" :password \"...\"}])"
  (:require [hive.events.interceptor :as interceptor]
            [hive.events.fx :as fx]
            [hive.events.cofx :as cofx]
            [hive.events.router :as router]
            [hive.events.fsm :as fsm]
            [hive.events.multi :as multi]
            [hive.events.log :as log]
            [hive.events.staleness :as staleness]))

;; Re-export core API
(defn ->interceptor [& args] (apply interceptor/->interceptor args))
#?(:clj (defn ->timed-interceptor [& args] (apply interceptor/->timed-interceptor args)))
(defn enqueue [& args] (apply interceptor/enqueue args))
(defn execute [& args] (apply interceptor/execute args))

(defn reg-fx [& args] (apply fx/reg-fx args))
(defn clear-fx [& args] (apply fx/clear-fx args))
(defn unreg-fx [& args] (apply fx/unreg-fx args))
(defn registered-fx-ids [& args] (apply fx/registered-fx-ids args))
(defn get-fx [& args] (apply fx/get-fx args))
(defn do-fx [& args] (apply fx/do-fx args))

;; The effect-invocation seam. An embedder installs its own policy (metrics,
;; tracing, a loud-fail counter for unregistered effects) without this library
;; knowing what any of those are, and without a second copy of do-fx.
(defn set-fx-executor! [& args] (apply fx/set-fx-executor! args))
(defn clear-fx-executor! [& args] (apply fx/clear-fx-executor! args))
(defn fx-executor-installed? [& args] (apply fx/fx-executor-installed? args))
(defn default-invoke-fx-handler [& args] (apply fx/default-invoke-fx-handler args))

(defn reg-cofx [& args] (apply cofx/reg-cofx args))
(defn inject-cofx [& args] (apply cofx/inject-cofx args))
(defn unreg-cofx [& args] (apply cofx/unreg-cofx args))
(defn registered-cofx-ids [& args] (apply cofx/registered-cofx-ids args))

(defn reg-event-fx [& args] (apply router/reg-event-fx args))
(defn reg-event-db [& args] (apply router/reg-event-db args))
(defn dispatch [& args] (apply router/dispatch args))
(defn dispatch-sync [& args] (apply router/dispatch-sync args))

;; Registry API. ONE store: a handler registered here is the same registration
;; a host's own dispatch sees, so an addon depends on this library rather than
;; on whichever host happens to be running it.
;; `event-registered?` and not `handler-registered?`: that name is already taken
;; below by the multimethod-dispatch subsystem, and a second def would silently
;; shadow it.
(defn get-event [& args] (apply router/get-event args))
(defn get-interceptors [& args] (apply router/get-interceptors args))
(defn event-registered? [& args] (apply router/handler-registered? args))
(defn registered-event-ids [& args] (apply router/registered-event-ids args))
(defn unreg-event [& args] (apply router/unreg-event args))
(defn append-interceptor! [& args] (apply router/append-interceptor! args))
(defn registry-snapshot [& args] (apply router/registry-snapshot args))
(defn restore-registry! [& args] (apply router/restore-registry! args))

;; Built-in interceptors
(def debug interceptor/debug)
(def trim-v interceptor/trim-v)

;; Multimethod-based dispatch (open extension)
(defn register-handler! [& args] (apply multi/register-handler! args))
(defn remove-handler! [& args] (apply multi/remove-handler! args))
(defn dispatch-multi [& args] (apply multi/dispatch-sync args))
(defn handler-registered? [& args] (apply multi/handler-registered? args))

;; FSM workflow engine (L2 deterministic workflows)
(defn fsm-compile [& args] (apply fsm/compile args))
(defn fsm-run [& args] (apply fsm/run args))
#?(:clj (defn fsm-run-async [& args] (apply fsm/run-async args)))
(defn fsm-step [& args] (apply fsm/step args))

;; FSM sub-FSM composition (nested workflows)
(defn fsm-run-sub [& args] (apply fsm/run-sub-fsm args))
(defn fsm-run-sub-fx [& args] (apply fsm/run-sub-fsm-fx args))
(defn fsm-sub-error? [& args] (apply fsm/sub-fsm-error? args))
(defn fsm-make-sub-handler [& args] (apply fsm/make-sub-fsm-handler args))

;; Logging configuration
(defn set-log-fn! [& args] (apply log/set-log-fn! args))

;; ============================================================================
;; Reload staleness
;; ============================================================================

(defn stale-registrations [& args] (apply staleness/stale-entries args))
