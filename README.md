# hive-events

Async event system for Clojure - re-frame patterns for JVM.

## Overview

`hive-events` brings re-frame's elegant event-driven architecture to Clojure/JVM with first-class async support via `core.async`. It provides:

- **Interceptor chain** - Composable middleware for event processing
- **Effects system** - Declarative side effects (`:db`, `:dispatch`, custom)
- **Coeffects system** - Dependency injection for pure handlers
- **Async dispatch** - Event queuing with `core.async` channels
- **Advanced patterns** - Debounce, throttle, retry with backoff, sagas

## Installation

Add to your `deps.edn`:

```clojure
{:deps {io.github.BuddhiLW/hive-events {:git/tag "v0.1.0" :git/sha "..."}}}
```

## Quick Start

```clojure
(require '[hive.events :as ev])

;; Initialize with your app state atom
(ev/init! (atom {:count 0}))

;; Register a simple event handler
(ev/reg-event-db :counter/inc
  (fn [db [_ amount]]
    (update db :count + (or amount 1))))

;; Dispatch events
(ev/dispatch [:counter/inc 5])
(ev/dispatch-sync [:counter/inc 1])  ; synchronous
```

## Event Handlers

Two types of handlers are supported:

### reg-event-db

For handlers that only update the database:

```clojure
(ev/reg-event-db :user/set-name
  (fn [db [_ name]]
    (assoc db :user-name name)))
```

### reg-event-fx

For handlers that produce multiple effects:

```clojure
(ev/reg-event-fx :user/fetch
  (fn [{:keys [db]} [_ user-id]]
    {:db (assoc db :loading? true)
     :http {:url (str "/api/users/" user-id)
            :on-success :user/fetch-success}}))
```

## Effects (fx)

Register custom effect handlers:

```clojure
(ev/reg-fx :http
  (fn [{:keys [url method on-success]}]
    ;; perform HTTP request
    ;; dispatch on-success event with response
    ))

(ev/reg-fx :local-storage
  (fn [{:keys [key value]}]
    (.setItem js/localStorage key value)))
```

Built-in effects:
- `:db` - Update application state
- `:dispatch` - Dispatch another event
- `:dispatch-n` - Dispatch multiple events
- `:dispatch-later` - Dispatch after delay

## Coeffects (cofx)

Inject dependencies into handlers:

```clojure
(ev/reg-cofx :now
  (fn [coeffects]
    (assoc coeffects :now (System/currentTimeMillis))))

(ev/reg-event-fx :log/timestamp
  [(ev/inject-cofx :now)]
  (fn [{:keys [db now]} _]
    {:db (assoc db :last-timestamp now)}))
```

Built-in coeffects:
- `:now` - Current timestamp
- `:random` - Random number
- `:uuid` - Random UUID string

## Interceptors

Compose cross-cutting concerns:

```clojure
(def validate-user
  (ev/->interceptor
    :id :validate-user
    :before (fn [ctx]
              (let [user (get-in ctx [:coeffects :db :user])]
                (if (valid? user)
                  ctx
                  (assoc ctx :effects {:dispatch [:error/invalid-user]}))))))

(ev/reg-event-fx :user/update
  [validate-user ev/debug]
  (fn [{:keys [db]} [_ changes]]
    {:db (update db :user merge changes)}))
```

Built-in interceptors:
- `debug` - Log event and effects
- `trim-v` - Remove event id from vector
- `path` - Focus handler on db path

## Async Extensions (JVM only)

```clojure
(require '[hive.events.async :as async])

;; Dispatch with timeout
(async/dispatch-with-timeout [:slow/operation] 5000)

;; Retry with exponential backoff
(async/dispatch-with-retry [:flaky/api-call]
  {:max-retries 3
   :initial-delay-ms 100
   :retry-pred (fn [ctx] (get-in ctx [:effects :error]))})

;; Debounced dispatch (search-as-you-type)
(async/dispatch-debounced :search [:search/query "foo"] 300)

;; Throttled dispatch (scroll handler)
(async/dispatch-throttled :scroll [:ui/position pos] 100)

;; Pub/sub event streams
(let [ch (async/subscribe-events :user/login)]
  (go-loop []
    (when-let [event (<! ch)]
      (println "Login:" event)
      (recur))))

;; Sagas for complex workflows
(async/saga [:order/created :payment/completed]
  (fn [event state]
    (case (first event)
      :order/created
      {:state (assoc state :order (second event))
       :dispatch [:payment/initiate (:id (second event))]}

      :payment/completed
      {:state (assoc state :paid? true)
       :dispatch [:fulfillment/start (:order-id state)]})))
```

## Architecture

```
hive.events (facade)
├── hive.events.interceptor  ; Interceptor chain
├── hive.events.fx           ; Effects registry
├── hive.events.cofx         ; Coeffects registry
├── hive.events.router       ; Event dispatch
└── hive.events.async        ; Async patterns (JVM)
```

## Design Principles

- **Events as data** - Events are vectors like `[:event-id & args]`
- **Pure handlers** - Handlers receive data, return data
- **Declarative effects** - Side effects described, not performed
- **Composable interceptors** - AOP-style middleware
- **Async-first** - Built on `core.async` for JVM

## Related Projects

- [re-frame](https://github.com/day8/re-frame) - Original inspiration (ClojureScript)
- [domino](https://github.com/domino-clj/domino) - Declarative data flow

## License

MIT
