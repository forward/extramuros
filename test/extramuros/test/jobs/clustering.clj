(ns extramuros.test.jobs.clustering
  (:use [incanter core])
  (:use [extramuros.test.configuration])
  (:use [extramuros.jobs.clustering])
  (:use [extramuros.jobs.core])
  (:use [extramuros hdfs datasets math])
  (:use [clojure.test])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))

;; Hadoop env setup
(setup-env)

(deftest canopy-native
  (println "\n*** canopy-native")
  (with-test-file "test_assets/test_clustering.csv" (def-schema :x *double* :y *double*)
    (fn [dataset]
      (let [job (make-job :canopy)]
        (set-config job {:output-path "test_assets/canopy"
                         :distance :euclidean
                         :t1 0.6
                         :t2 0.4
                         :should-cluster true
                         :table dataset})
        (run job)
        (is (= 2 (count (output job :clusters))))
        (is (= 3 (count (get (output job :folded-points) "0"))))
        (is (= 3 (count (get (output job :folded-points) "1"))))
        (is (= 2 (count (output job :folded-points))))
        (is (= org.jfree.chart.JFreeChart (class (visualize job {:x-label "x" :y-label "y"}))))
        (doseq [chart (visualize job)]
          (is (= org.jfree.chart.JFreeChart (class chart))))
        (delete (output-path job))))))

(deftest canopy-text
  (println "\n*** canopy-text")
  (with-text-test-file (def-schema :x *double* :y *double*)
    (fn [wrtr]
      (.println wrtr "0.1,0.1")
      (.println wrtr "0.2,0.2")
      (.println wrtr "0.3,0.3")
      (.println wrtr "50.0,50.0")
      (.println wrtr "50.1,50.1")
      (.println wrtr "49.9,49.9"))
    (fn [dataset]
      (let [job (make-job :canopy)]
        (set-config job {:output-path "test_assets/canopy"
                         :distance :euclidean
                         :t1 0.6
                         :t2 0.4
                         :should-cluster true
                         :table dataset})
        (run job)
        (is (= 2 (count (output job :clusters))))
        (is (= 3 (count (get (output job :folded-points) "0"))))
        (is (= 3 (count (get (output job :folded-points) "1"))))
        (is (= 2 (count (output job :folded-points))))
        (is (= org.jfree.chart.JFreeChart (class (visualize job {:x-label "x" :y-label "y"}))))
        (doseq [chart (visualize job)]
          (is (= org.jfree.chart.JFreeChart (class chart))))
        (delete (output-path job))))))
