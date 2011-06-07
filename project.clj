(defproject extramuros "1.0.0-SNAPSHOT"
  :description "Statistical computing using mahout and incanter"
  :jvm-opts ["-Xmx1g -server"] 
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [incanter "1.2.3"]
                 [org.apache.mahout/mahout-core "0.4"]
                 [org.apache.mahout/mahout-utils "0.4"]
                 [cascading/cascading-core "1.2.4-wip-85"]
                 [clojure-hadoop "1.3.1-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :repositories {"conjars.org" "http://conjars.org/repo"})
