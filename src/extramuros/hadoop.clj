(ns extramuros.hadoop
  (:require [clojure-hadoop.gen :as gen]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.config :as config]
            [clojure-hadoop.wrap :as wrap])
  (:import (java.util StringTokenizer)
           (org.apache.hadoop.util Tool))
  (:use clojure.test))

(imp/import-conf)
(imp/import-io)
(imp/import-fs)
(imp/import-mapreduce)
(imp/import-mapreduce-lib)

(gen/gen-job-classes)
(gen/gen-main-method)

(def mapper-map
     (wrap/wrap-map
      (fn [key value]
        (map (fn [token] [token 1])
             (enumeration-seq (StringTokenizer. value))))
      wrap/int-string-map-reader))

(def reducer-reduce
     (wrap/wrap-reduce
      (fn [key values-fn]
        [[key (reduce + (values-fn))]])))

(defn configuration-obj
  ([] (Configuration.))
  ([path-str & others]
     (let [conf (Configuration.)]
       (.addResource conf (Path. path-str))
       (loop [pstr others]
         (if (empty? others)
           conf
           (do (.addResource conf (Path. (first pstr)))
               (recur (rest pstr))))))))


(defn tool-run [^Tool this args]
  (let [job (Job.)
        conf (config/configuration job)
        _ (.addResource conf "/Users/antonio/Development/tmp/hadoop-0.20.2/conf/core-site.xml")
        conf (.getConf this)
        _ (.addResource conf "/Users/antonio/Development/tmp/hadoop-0.20.2/conf/core-site.xml")]
    (println (str "ADDING RESOURCE:" conf))
    (doto job
      (.setJarByClass (.getClass this))
      (.setJobName "wordcount2")
      (.setOutputKeyClass Text)
      (.setOutputValueClass Text)
      (.setMapperClass (Class/forName "clojure_hadoop.examples.wordcount2_mapper"))
      (.setReducerClass (Class/forName "clojure_hadoop.examples.wordcount2_reducer"))
      (.setInputFormatClass TextInputFormat)
      (.setOutputFormatClass TextOutputFormat)
      (FileInputFormat/setInputPaths ^String (first args))
      (FileOutputFormat/setOutputPath (Path. (second args)))
      (.waitForCompletion true)))
  0)

(defn test-wordcount-2  []
  (.delete (FileSystem/get (configuration-obj "/Users/antonio/Development/tmp/hadoop-0.20.2/conf/core-site.xml")) (Path. "clojurehadoop.out") true)
  (println (vec (.listStatus (FileSystem/get (configuration-obj "/Users/antonio/Development/tmp/hadoop-0.20.2/conf/core-site.xml")) (Path. "/user/antonio"))))
  (tool-run (clojure_hadoop.job.) ["hdfs://localhost:9000/user/antonio/README.txt" "hdfs://localhost:9000/user/antonio/clojurehadoop.out"]))


;; (ns extramuros.hadoop
;;   (:require [clojure-hadoop.wrap :as wrap]
;;             [clojure-hadoop.defjob :as defjob]
;;             [clojure-hadoop.config :as config]
;;             [clojure-hadoop.imports :as imp])
;;   (:use clojure.test clojure-hadoop.job))

;; (imp/import-io)
;; (imp/import-mapreduce)

;; (defn my-map [key value]
;;   ["line" 1])

;; (defn my-reduce [key value-fn]
;;   ([key (reduce + value-fn)]))

;; (defn string-long-writer [^TaskInputOutputContext context ^String key value]
;;   (.write context (Text. key) (LongWritable. value)))

;; (defn string-long-reduce-reader [^Text key wvalues]
;;   [(.toString key)
;;    (fn [] (map (fn [^LongWritable v] (.get v)) wvalues))])


;; (defjob/defjob job
;;   :map my-map
;;   :map-reader wrap/string-map-reader
;;   :map-writer string-long-writer
;;   :reduce my-reduce
;;   :reduce-reader string-long-reduce-reader
;;   :reduce-writer string-long-writer
;;   :output-key Text
;;   :output-value LongWritable
;;   :input-format :text
;;   :output-format :text
;;   :compress-output false
;;   :input "g50.csv"
;;   :output "clojurehadoop.out"
;;   :replace true)

;; ;;(deftest test-wordcount-5  (is (run job)))

;; (defn test-run
;;   ([]
;; ;     (let [conf (config/configuration job)]
;; ;       (.addResource conf "/Users/antonio/Development/tmp/hadoop-0.20.2/conf/core-site.xml")
;; ;       (run job))
;;      (run job)))

