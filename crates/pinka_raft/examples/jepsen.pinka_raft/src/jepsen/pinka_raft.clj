(ns jepsen.pinka-raft
  (:require [cheshire.core :refer :all]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clj-http.client :as http]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.linearizable-register :as lr]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]])
  (:import [knossos.model Model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn servers
  "Return a list of servers"
  [test]
  (->> (:nodes test)
       (str/join ",")))

(defn db
  "Pinka-Raft test database"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "Setting up Pinka-Raft test database")
      (cu/start-daemon! {:env {:PATH "/usr/local/bin:/usr/bin:/bin:/sbin:/usr/sbin"}
                         :logfile "/home/admin/log.txt"
                         :pidfile "/home/admin/pid.txt"
                         :chdir "/home/admin"}
                        "/home/admin/simple-register" :--name node :--servers (servers test))
      (loop []
        (let [response (try
                         (http/get (str "http://" node ":8080/init") {:as :json})
                         (catch Exception e
                           (warn "Failed to initialize, retrying...")
                           nil))]
          (if response
            (info node "Key returned a value, setup complete")
            (do (Thread/sleep 1000)
                (recur))))))
    (teardown! [_ test node]
      (info node "Tearing down Pinka-Raft test database")
      (cu/stop-daemon! "simple-register" "/home/admin/pid.txt"))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (info node "Opening client connection")
    (assoc this :conn node))
  (setup! [this test])
  (invoke! [this test op]
    (let [v (:value op)
          url (str "http://" (:conn this) ":8080")]
      (case (:f op)
        :read (assoc op :type :ok :value
                     (:value (http/get (str url "/foo") {:as :json})))
        :write (do (info "Writing" (generate-string {:type "Write" :value ["foo" v]}))
                   (assoc op :type :ok :value
                          (do (http/post url
                                         {:body (generate-string {:type "Write" :value ["foo" v]})
                                          :content-type :json
                                          :as :json})
                              nil)))
        :cas (try+ (let [[v v'] (:value op)]
                     (assoc op :type :ok :value
                            (do (http/post url
                                           {:body (generate-string {:type "Cas" :value ["foo" v v']})
                                            :content-type :json
                                            :as :json})
                                nil)))
                   (catch [:status 412] e
                     (assoc op :type :fail))))))
  (teardown! [this test])
  (close! [_ test]))

(defn pinka-raft-test
  "Run a Jepsen test for Pinka-Raft"
  [opts]
  (merge  tests/noop-test
          opts
          {:pure-generators true
           :name   "pinka-raft"
           :db     (db "0.1.0-beta.1")
           :os     debian/os
           :client (Client. nil)
           :nemesis         (nemesis/partition-random-halves)
           :checker         (checker/compose
                             {:linear (checker/linearizable
                                       {:model     (model/cas-register)
                                        :algorithm :linear})
                              :timeline (timeline/html)})
           :generator (->> (gen/mix [r w cas])
                           (gen/stagger 1/2)
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}]))
                           (gen/time-limit (:time-limit opts)))}))

(defn -main
  "Handle command line arguments"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn pinka-raft-test})
                   (cli/serve-cmd))
            args))
