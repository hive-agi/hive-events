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
            [hive.events.log :as log]))

;; Re-export core API
(def ->interceptor interceptor/->interceptor)
#?(:clj (def ->timed-interceptor interceptor/->timed-interceptor))
(def enqueue interceptor/enqueue)
(def execute interceptor/execute)

(def reg-fx fx/reg-fx)
(def clear-fx fx/clear-fx)
(def unreg-fx fx/unreg-fx)
(def registered-fx-ids fx/registered-fx-ids)
(def get-fx fx/get-fx)
(def do-fx fx/do-fx)

;; The effect-invocation seam. An embedder installs its own policy (metrics,
;; tracing, a loud-fail counter for unregistered effects) without this library
;; knowing what any of those are, and without a second copy of do-fx.
(def set-fx-executor! fx/set-fx-executor!)
(def clear-fx-executor! fx/clear-fx-executor!)
(def fx-executor-installed? fx/fx-executor-installed?)
(def default-invoke-fx-handler fx/default-invoke-fx-handler)

(def reg-cofx cofx/reg-cofx)
(def inject-cofx cofx/inject-cofx)
(def unreg-cofx cofx/unreg-cofx)
(def registered-cofx-ids cofx/registered-cofx-ids)

(def reg-event-fx router/reg-event-fx)
(def reg-event-db router/reg-event-db)
(def dispatch router/dispatch)
(def dispatch-sync router/dispatch-sync)

;; Registry API. ONE store: a handler registered here is the same registration
;; a host's own dispatch sees, so an addon depends on this library rather than
;; on whichever host happens to be running it.
;; `event-registered?` and not `handler-registered?`: that name is already taken
;; below by the multimethod-dispatch subsystem, and a second def would silently
;; shadow it.
(def get-event router/get-event)
(def get-interceptors router/get-interceptors)
(def event-registered? router/handler-registered?)
(def registered-event-ids router/registered-event-ids)
(def unreg-event router/unreg-event)
(def append-interceptor! router/append-interceptor!)
(def registry-snapshot router/registry-snapshot)
(def restore-registry! router/restore-registry!)

;; Built-in interceptors
(def debug interceptor/debug)
(def trim-v interceptor/trim-v)

;; Multimethod-based dispatch (open extension)
(def register-handler! multi/register-handler!)
(def remove-handler! multi/remove-handler!)
(def dispatch-multi multi/dispatch-sync)
(def handler-registered? multi/handler-registered?)

;; FSM workflow engine (L2 deterministic workflows)
(def fsm-compile fsm/compile)
(def fsm-run fsm/run)
#?(:clj (def fsm-run-async fsm/run-async))
(def fsm-step fsm/step)

;; FSM sub-FSM composition (nested workflows)
(def fsm-run-sub fsm/run-sub-fsm)
(def fsm-run-sub-fx fsm/run-sub-fsm-fx)
(def fsm-sub-error? fsm/sub-fsm-error?)
(def fsm-make-sub-handler fsm/make-sub-fsm-handler)

;; Logging configuration
(def set-log-fn! log/set-log-fn!)
