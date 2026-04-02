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
(def enqueue interceptor/enqueue)
(def execute interceptor/execute)

(def reg-fx fx/reg-fx)
(def clear-fx fx/clear-fx)
(def unreg-fx fx/unreg-fx)
(def registered-fx-ids fx/registered-fx-ids)
(def do-fx fx/do-fx)

(def reg-cofx cofx/reg-cofx)
(def inject-cofx cofx/inject-cofx)
(def unreg-cofx cofx/unreg-cofx)
(def registered-cofx-ids cofx/registered-cofx-ids)

(def reg-event-fx router/reg-event-fx)
(def reg-event-db router/reg-event-db)
(def dispatch router/dispatch)
(def dispatch-sync router/dispatch-sync)

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
