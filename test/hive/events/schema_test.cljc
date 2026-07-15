(ns hive.events.schema-test
  "reg-event-schema coerces+validates an event's payload against ONE registered
   malli schema-key via a :before interceptor, before the handler runs."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-spi.schema.registry :as sreg]
            [hive.events.schema :as es]))

(sreg/register! ::ev-args [:map [:n :int]])

(deftest coerce-event-interceptor-coerces-and-guards
  (let [ic (es/coerce-event-interceptor ::ev-args)]
    (testing "a valid payload is coerced and threaded back into the event"
      (is (= [:e {:n 5}]
             (get-in ((:before ic) {:coeffects {:event [:e {:n 5}]}})
                     [:coeffects :event]))))
    (testing "an invalid payload is refused (schema/invalid) before the handler"
      (is (= :schema/invalid
             (try ((:before ic) {:coeffects {:event [:e {:n "x"}]}}) :no-throw
                  (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))))))

(deftest reg-event-schema-prepends-the-coerce-interceptor
  (let [registry (es/reg-event-schema ::my-ev ::ev-args
                   (fn [_cofx event] {:doubled (* 2 (:n (second event)))}))
        entry    (get registry ::my-ev)]
    (testing "the event is registered with the schema-coerce interceptor first"
      (is (some? entry))
      (is (= :schema-coerce (:id (first (:interceptors entry)))))
      (is (some #(= :handler (:id %)) (:interceptors entry))))))
