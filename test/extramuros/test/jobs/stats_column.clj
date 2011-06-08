(ns extramuros.test.jobs.stats-column
  (:use [incanter core])
  (:use [extramuros.test.configuration])
  (:use [extramuros.jobs.stats-column])
  (:use [extramuros.jobs.core])
  (:use [extramuros hdfs datasets math])
  (:use [clojure.test])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))

;; Hadoop env setup
(setup-env)

(deftest centrality-native
  (println "\n*** centrality-native")
  (with-test-file "test_assets/test_input_stats.csv" (def-schema :vals *integer*)
    (fn [dataset]
      (let [job (make-job :centrality-stats)]
        (set-config job {:output-path "test_assets/test_centrality.txt"
                         :column "vals"
                         :table dataset})
        (run job)
        (is (= 1 (get (output job) "min")))
        (is (= 10 (get (output job) "max")))
        (is (= 5.5 (get (output job) "avg")))        
        (delete (output-path job))))))


(deftest centrality-text
  (println "\n*** centrality-text")
  (with-text-test-file (def-schema :vals *integer*)
    (fn [wrtr]
      (.println wrtr "1")
      (.println wrtr "2")
      (.println wrtr "3")
      (.println wrtr "4")
      (.println wrtr "5")
      (.println wrtr "6")
      (.println wrtr "7")
      (.println wrtr "8")
      (.println wrtr "9")
      (.println wrtr "10"))
    (fn [dataset]
      (let [job (make-job :centrality-stats)]
        (set-config job {:output-path "test_assets/test_dispersion.txt"
                         :column "vals"
                         :table dataset})
        (run job)
        (is (= 1 (get (output job) "min")))
        (is (= 10 (get (output job) "max")))
        (is (= 5.5 (get (output job) "avg")))
        (= clojure.lang.PersistentVector (class (visualize job)))
        (delete (output-path job))))))

(deftest dispersion-native
  (println "\n*** dispersion-native")
  (with-test-file "test_assets/test_input_stats.csv" (def-schema :vals *integer*)
    (fn [dataset]
      (let [job (make-job :dispersion-stats)]
        (set-config job {:output-path "test_assets/test_dispersion.txt"
                         :column "vals"
                         :average 5.5
                         :table dataset})
        (run job)
        (is (= 8.25 (get (output job) "var")))
        (is (= 3 (Math/ceil  (get (output job) "stdev"))))
        (= clojure.lang.PersistentVector (class (visualize job)))
        (delete (output-path job))))))

(deftest dispersion-text
  (println "\n*** dispersion-text")
  (with-text-test-file (def-schema :vals *integer*)
    (fn [wrtr]
      (.println wrtr "1")
      (.println wrtr "2")
      (.println wrtr "3")
      (.println wrtr "4")
      (.println wrtr "5")
      (.println wrtr "6")
      (.println wrtr "7")
      (.println wrtr "8")
      (.println wrtr "9")
      (.println wrtr "10"))
    (fn [dataset]
      (let [job (make-job :dispersion-stats)]
        (set-config job {:output-path "test_assets/test_centrality.txt"
                         :column "vals"
                         :average 5.5
                         :table dataset})
        (run job)
        (is (= 8.25 (get (output job) "var")))
        (is (= 3 (Math/ceil  (get (output job) "stdev"))))
        (= clojure.lang.PersistentVector (class (visualize job)))
        (delete (output-path job))))))

(deftest frequency-distribution-native
  (println "\n*** frequency-distribution-native")
  (with-test-file "test_assets/test_input_stats.csv" (def-schema :vals *integer*)
    (fn [dataset]
      (let [job (make-job :frequency-distribution)]
        (set-config job {:output-path "test_assets/freq_distribution.txt"
                         :column "vals"
                         :table dataset})
        (run job)
        (doseq [p (iterator-seq (output job))]
          (let [[v c] (pair-to-seq p)]
            (is (= (parse-writable c) 1))))
        (is (= org.jfree.chart.JFreeChart (class (visualize job))))
        (delete (output-path job))))))

(deftest frequency-distribution-text
  (println "\n*** frequency-distribution-text")
  (with-text-test-file (def-schema :vals *integer*)
    (fn [wrtr]
      (.println wrtr "1")
      (.println wrtr "2")
      (.println wrtr "3")
      (.println wrtr "4")
      (.println wrtr "5")
      (.println wrtr "6")
      (.println wrtr "7")
      (.println wrtr "8")
      (.println wrtr "9")
      (.println wrtr "10"))
    (fn [dataset]
      (let [job (make-job :frequency-distribution)]
        (set-config job {:output-path "test_assets/test_frequency_dist.txt"
                         :column "vals"
                         :table dataset})
        (run job)
        (doseq [p (iterator-seq (output job))]
          (let [[v c] (pair-to-seq p)]
            (is (= (parse-writable c) 1))))
        (is (= org.jfree.chart.JFreeChart (class (visualize job))))
        (delete (output-path job))))))

(deftest table-stats-native
  (println "\n*** table-stats-native")
  (with-test-file "test_assets/test_input_stats.csv" (def-schema :vals *integer*)
    (fn [dataset]
      (let [job (make-job :table-stats)]
        (set-config job {:output-path "test_assets/table_stats.txt"
                         :table dataset})
        (run job)
        (is (= (:average (get (output job) "vals")) 5.5))
        (is (= (:min (get (output job) "vals")) 1))
        (is (= (:max (get (output job) "vals")) 10))
        (is (= (:variance (get (output job) "vals")) 8.25))
        (is (= (Math/ceil (:standard-deviation (get (output job) "vals"))) 3))
        (is (seq? (visualize job)))
        (delete (output-path job))))))

(deftest table-stats-text
  (println "\n*** table-stats-text")
  (with-text-test-file (def-schema :vals *integer*)
    (fn [wrtr]
      (.println wrtr "1")
      (.println wrtr "2")
      (.println wrtr "3")
      (.println wrtr "4")
      (.println wrtr "5")
      (.println wrtr "6")
      (.println wrtr "7")
      (.println wrtr "8")
      (.println wrtr "9")
      (.println wrtr "10"))
    (fn [dataset]
      (let [job (make-job :table-stats)]
        (set-config job {:output-path "test_assets/table_stats.txt"
                         :table dataset})
        (run job)
        (is (= (:average (get (output job) "vals")) 5.5))
        (is (= (:min (get (output job) "vals")) 1))
        (is (= (:max (get (output job) "vals")) 10))
        (is (= (:variance (get (output job) "vals")) 8.25))
        (is (= (Math/ceil (:standard-deviation (get (output job) "vals"))) 3))
        (is (seq? (visualize job)))
         (delete (output-path job))))))

(deftest normalization-native
  (println "\n*** normalization-native")
  (with-test-file "test_assets/test_input.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :normalization)]
        (set-config job {:output-path "test_assets/normalization"
                         :columns ["columna" "columnb"]
                         :min-values [1 1.0]
                         :max-values [3 3.0]
                         :table dataset})
        (run job)
        (is (= [0.0 0.0] (row-to-seq (first (table-rows (output job))))))
        (is (= [0.5 0.5] (row-to-seq (second (table-rows (output job))))))
        (is (= [1.0 1.0] (row-to-seq (nth (table-rows (output job)) 2))))
        (delete (output-path job))))))

(deftest table-stats-text
  (println "\n*** table-stats-text")
  (with-text-test-file (def-schema :vals *integer*)
    (fn [wrtr]
      (.println wrtr "1")
      (.println wrtr "2")
      (.println wrtr "3")
      (.println wrtr "4")
      (.println wrtr "5")
      (.println wrtr "6")
      (.println wrtr "7")
      (.println wrtr "8")
      (.println wrtr "9")
      (.println wrtr "10"))
    (fn [dataset]
      (let [job (make-job :normalization)]
        (set-config job {:output-path "test_assets/normalization"
                         :columns ["vals"]
                         :min-values [1]
                         :max-values [10]
                         :table dataset})
        (run job)
        (let  [rows (map row-to-seq (table-rows (output job)))]
          (is (= 0.0 (first (first rows))))
          (is (= 5 (Math/floor (* 10 (first (nth rows 5))))))
          (is (= 1.0 (first (last rows)))))
        (delete (output-path job))))))
