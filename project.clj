(defproject extramuros "1.0.0-SNAPSHOT"
  :description "Statistical computing using mahout and incanter"
  :jvm-opts ["-Xmx1g -server"] 
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [incanter "1.2.3"]
                 [joda-time/joda-time "1.6.2"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :repositories {"conjars.org" "http://conjars.org/repo"})
