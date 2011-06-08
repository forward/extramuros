(ns extramuros.test.jobs.file
  (:use [extramuros.test.configuration])
  (:use [extramuros.jobs.file])
  (:use [extramuros.jobs.core])
  (:use [extramuros hdfs datasets])
  (:use [clojure.test])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))


;; Hadoop env setup
(setup-env)


(deftest countlinest-native
  (println "\n*** countlinest-native")
  (with-test-file "test_assets/test_input.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :table-count-lines)]
        (set-config job {:output-path "test_assets/test_countlines.txt"
                         :table dataset})
        (run job)
        (is (= 3 (output job)))
        (delete (output-path job))))))


(deftest countlinest-text-file
  (println "\n*** countlinest-text-file")
  (with-default-text-test-file
    (fn [dataset]
      (let [job (make-job :table-count-lines)]
        (set-config job {:output-path "test_assets/test_countlines.txt"
                         :table dataset})
        (run job)
        (is (= 3 (output job)))
        (delete (output-path job))
        (delete "test_assets/test_countlines.txt")))))


(deftest countlinest-text-file-nulls
  (println "\n*** countlinest-text-file-nulls")
  (with-text-test-file *default-test-schema*
    (fn [writer]
      (.println writer "NULL,NULL,NULL")
      (.println writer "\"one\",1,1.0"))
    (fn [dataset]
      (let [job (make-job :table-count-lines)]
        (set-config job {:output-path "test_assets/test_countlines.txt"
                         :table dataset})
        (run job)
        (is (= 2 (output job)))
        (delete (output-path job))
        (delete "test_assets/test_countlines.txt")))))

(deftest sample-native
  (println "\n*** sample-native")
  (when (exists? "test_assets/test_sample.txt") (delete "test_assets/test_sample.txt"))
  (with-test-file "test_assets/test_input_ten_lines.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :sample-file)]
        (set-config job {:file-to-sample (table-rows-file dataset)
                         :file-output "test_assets/test_sample.txt"
                         :table dataset
                         :total-lines 10
                         :percentage-to-sample 0.2})
        (run job)
        (is (= 2 (count (iterator-seq (output job)))))
        (delete (output-path job))))))


(deftest probabilistic-sample-table-native
  (println "\n*** probabilistic-sample-table-native")
  (when (exists? "test_assets/test_probabilistic_sample.txt") (delete "test_assets/test_probabilistic_sample.txt"))
  (with-test-file "test_assets/test_input_ten_lines.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :probabilistic-sample-table)]
        (set-config job {:directory-output "test_assets/test_probabilistic_sample.txt"
                         :sampling-probability 0.5
                         :table dataset})
        (run job)
        (is (> (count (table-rows (output job))) 0))
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest probabilistic-sample-table-text
  (println "\n*** probabilistic-sample-table-text")
  (when (exists? "test_assets/test_probabilistic_sample.txt") (delete "test_assets/test_probabilistic_sample.txt"))
  (with-text-test-file *default-test-schema*
    (fn [writer]
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0")
      (.println writer "\"one\",1,1.0"))
    (fn [dataset]
      (let [job (make-job :probabilistic-sample-table)]
        (set-config job {:directory-output "test_assets/test_probabilistic_sample.txt"
                         :sampling-probability 0.5
                         :table dataset})
        (run job)
        
        (is (> (count (table-rows (output job))) 0))
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest probabilistic-sample-table-text-nulls
  (println "\n*** probabilistic-sample-table-text-nulls")
  (when (exists? "test_assets/test_probabilistic_sample.txt") (delete "test_assets/test_probabilistic_sample.txt"))
  (with-text-test-file *default-test-schema*
    (fn [writer]
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,")
      (.println writer ",,"))
    (fn [dataset]
      (let [job (make-job :probabilistic-sample-table)]
        (set-config job {:directory-output "test_assets/test_probabilistic_sample.txt"
                         :sampling-probability 0.5
                         :table dataset})
        (run job)
        
        (is (> (count (table-rows (output job))) 0))
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest vectorize-table-native
  (println "\n*** vectorize-table")
  (when (exists? "test_assets/test_vectorize.txt") (delete "test_assets/test_vectorize.txt"))
  (with-test-file "test_assets/test_input.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :vectorize-table)]
        (set-config job {:directory-output "test_assets/test_vectorize.txt"
                         :column-names ["columna" "columnb"]
                         :vector-type :dense
                         :table dataset})
        (run job)
        (is (= (count (table-rows (output job))) 3))
        (let [first-row (row-to-seq (first (table-rows dataset)))
              second-row (row-to-seq (second (table-rows dataset)))
              third-row (row-to-seq (nth (table-rows dataset) 2))]

          (is (= 1 (nth first-row 1)))
          (is (= 1 (nth first-row 2)))

          (is (= 2 (nth second-row 1)))
          (is (= 2 (nth second-row 2)))
          
          (is (= 3 (nth third-row 1)))
          (is (= 3 (nth third-row 2))))
        
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest vectorize-table-text
  (println "\n*** vectorize-table-text")
  (when (exists? "test_assets/test_vectorize.txt") (delete "test_assets/test_vectorize.txt"))
  (with-default-text-test-file
    (fn [dataset]
      (let [job (make-job :vectorize-table)]
        (set-config job {:directory-output "test_assets/test_vectorize.txt"
                         :column-names ["columna" "columnb"]
                         :vector-type :dense
                         :table dataset})
        (run job)
        (is (= (count (table-rows (output job))) 3))
        (let [first-row (row-to-seq (first (table-rows dataset)))
              second-row (row-to-seq (second (table-rows dataset)))
              third-row (row-to-seq (nth (table-rows dataset) 2))]

          (is (= 1 (nth first-row 1)))
          (is (= 1 (nth first-row 2)))

          (is (= 2 (nth second-row 1)))
          (is (= 2 (nth second-row 2)))
          
          (is (= 3 (nth third-row 1)))
          (is (= 3 (nth third-row 2))))
        
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest vectorize-table-native-nulls
  (println "\n*** vectorize-table-native-nulls")
  (when (exists? "test_assets/test_vectorize.txt") (delete "test_assets/test_vectorize.txt"))
  (with-test-file "test_assets/test_input_nulls.csv" *default-test-schema*
    (fn [dataset]
      (let [job (make-job :vectorize-table)]
        (set-config job {:directory-output "test_assets/test_vectorize.txt"
                         :column-names ["columna" "columnb"]
                         :vector-type :dense
                         :table dataset})
        (run job)
        (is (= (count (table-rows (output job))) 1))
        
        (delete (output-path job))
        (delete (table-file (output job)))))))

(deftest vectorize-table-text-nulls
  (println "\n*** vectorize-table-text")
  (when (exists? "test_assets/test_vectorize.txt") (delete "test_assets/test_vectorize.txt"))
  (with-text-test-file
    *default-test-schema*
    (fn [wrtr]
      (.println wrtr "\"one\",1,1.0")
      (.println wrtr "\"two\",2,Null")
      (.println wrtr "\"three\",,3.0"))
    (fn [dataset]
      (let [job (make-job :vectorize-table)]
        (set-config job {:directory-output "test_assets/test_vectorize.txt"
                         :column-names ["columna" "columnb"]
                         :vector-type :dense
                         :table dataset})
        (run job)
        (is (= (count (table-rows (output job))) 1))
        (let [first-row (row-to-seq (first (table-rows dataset)))]

          (is (= 1 (nth first-row 1)))
          (is (= 1 (nth first-row 2))))
        
        (delete (output-path job))
        (delete (table-file (output job)))))))

