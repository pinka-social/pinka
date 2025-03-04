(defproject jepsen.pinka_raft "0.1.0-beta.1"
  :description "A Jepsen test for Pinka-Raft"
  :main jepsen.pinka-raft
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.7"]
                 [cheshire "5.13.0"]
                 [clj-http "3.13.0"]]
  :repl-options {:init-ns jepsen.pinka-raft})
