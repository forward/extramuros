(ns extramuros.test.configuration
  (:use [extramuros hdfs datasets])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))

(defn setup-env
  ([] (bootstrap!)))

(def *default-test-schema* (def-schema :name *string* :columna *integer* :columnb *float*))

(defn with-test-file
  ([test-file test-file-schema test-fn]
     (let [test-file-schema# test-file-schema]
       (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
       (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))
       (let [dataset (import-dataset test-file "test_assets/imported_out.csv" test-file-schema#)]
         (try (test-fn dataset)
              (finally
               (do
               (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
         (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows")))))))))

(defn with-default-text-test-file
  ([test-fn]
     (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)
        fsos (.create (FileSystem/get *conf*) (path "test_assets/test.txt"))
        wrtr (java.io.PrintWriter. fsos)]
    (.println wrtr "\"one\",1,1.0")
    (.println wrtr "\"two\",2,2.0")
    (.println wrtr "\"three\",3,3.0")    
    (.flush wrtr)
    (.close wrtr)
    (let [table (wrap-dataset :text "test_assets/test.txt" "test_assets/test.txt.out" test-file-schema {:separator ","})]
      (test-fn table))
    (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
    (when (exists? "test_assets/test.txt.out") (delete "test_assets/test.txt.out")))))

(defn with-text-test-file
  ([schema writer-fn test-fn]
     (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
  (let [test-file-schema schema
        fsos (.create (FileSystem/get *conf*) (path "test_assets/test.txt"))
        wrtr (java.io.PrintWriter. fsos)]
    (writer-fn wrtr)
    (.flush wrtr)
    (.close wrtr)
    (let [table (wrap-dataset :text "test_assets/test.txt" "test_assets/test.txt.out" test-file-schema {:separator ","})]
      (test-fn table))
    (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
    (when (exists? "test_assets/test.txt.out") (delete "test_assets/test.txt.out")))))
