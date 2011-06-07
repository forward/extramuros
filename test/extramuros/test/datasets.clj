(ns extramuros.test.datasets
  (:use [extramuros.datasets])
  (:use [extramuros.hdfs])
  (:use [clojure.test])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))

;; Hadoop env setup
(bootstrap!)

(deftest parse-datum-string
  (let [v (parse-datum "hi" *string*)]
    (is (= v "hi"))
    (is (= (class v) String))))

(deftest parse-datum-float
  (let [v (parse-datum "2.0" *float*)]
    (is (= v 2.0))
    (is (= (class v) Float))))

(deftest parse-datum-integer
  (let [v (parse-datum "2" *integer*)]
    (is (= v 2))
    (is (= (class v) Integer))))

(deftest parse-datum-double
  (let [v (parse-datum "2" *double*)]
    (is (= v 2))
    (is (= (class v) Double))))

(deftest parse-datum-categorical
  (let [v (parse-datum "a" *categorical*)]
    (is (= v "a"))
    (is (= (class v) String))))

(deftest test-make-row
  (let [schema (def-schema :a *integer* :b *string*)
        id 0
        data ["1" "b"]
        row (make-row id schema data)]
    (is (= (.getId row) 0))
    (is (= ["a" "b"] (.getColumnsNames row)))
    (is (= [*integer* *string*] (.getColumnsTypes row)))
    (is (= [1 "b"] (.getValues row)))))

(deftest test-ordered-types-schema
  (let [schema (def-schema :a *integer* :b *string* :c *integer*)
        mapped (ordered-types-schema schema)]
    (is (= *integer* (nth mapped 0)))
    (is (= *string* (nth mapped 1)))
    (is (= *integer* (nth mapped 2)))))

(deftest test-parse-writable
  (let [iw (org.apache.hadoop.io.IntWritable. 1)
        lw (org.apache.hadoop.io.LongWritable. 2)
        tw (org.apache.hadoop.io.Text. "test")]
    (is (= 1 (parse-writable iw)))
    (is (= 2 (parse-writable lw)))
    (is (= "test" (parse-writable tw)))))


(deftest test-make-table-header
  (let [schema (def-schema :a *integer* :b *string* :c *integer*)
        header (make-table-header schema)
        columns (.getColumnNames header)]
    (is (= "a" (nth columns 0)))
    (is (= "b" (nth columns 1)))
    (is (= "c" (nth columns 2)))))

(deftest test-import-dataset
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)]
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))
    (let [dataset (import-dataset "test_assets/test_input.csv" "test_assets/imported_out.csv" test-file-schema)]
      (is (exists? "test_assets/imported_out.csv"))
      (is (exists? "test_assets/imported_out.csv"))
      (is (= 3 (count (table-rows dataset)))))
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))))

(deftest test-import-dataset-separator
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)
        _ (println "ERROR HERE")]
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))
    (let [dataset (import-dataset "test_assets/test_input.tsv" "test_assets/imported_out.csv" test-file-schema
                                  :delim "\\.")]
      (is (exists? "test_assets/imported_out.csv"))
      (is (exists? "test_assets/imported_out.csv"))
      (is (= 3 (count (table-rows dataset)))))
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))))

(deftest test-import-dataset-skip-header
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)]
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))
    (let [dataset (import-dataset "test_assets/test_input_title.csv" "test_assets/imported_out.csv" test-file-schema :skip 1)]
      (is (exists? "test_assets/imported_out.csv"))
      (is (exists? "test_assets/imported_out.csv"))
      (is (= 3 (count (table-rows dataset)))))
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))))

(deftest test-open-dataset
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)]
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))
    (let [dataset (import-dataset "test_assets/test_input.csv" "test_assets/imported_out.csv" test-file-schema)]
      (is (exists? "test_assets/imported_out.csv"))
      (is (exists? "test_assets/imported_out.csv"))
      (is (= 3 (count (table-rows dataset)))))
    (let [table (open-dataset "test_assets/imported_out.csv")]
      (is (= 3 (count (table-rows table)))))
    (when (exists? "test_assets/imported_out.csv") (delete "test_assets/imported_out.csv"))
    (when (exists? "test_assets/imported_out.csv.rows") (delete "test_assets/imported_out.csv.rows"))))

(deftest test-wrap-text-dataset
  (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
  (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *float*)
        fsos (.create (FileSystem/get *conf*) (path "test_assets/test.txt"))
        wrtr (java.io.PrintWriter. fsos)]
    (.println wrtr "\"one\",1,1.0")
    (.println wrtr "\"two\",2,2.0")
    (.flush wrtr)
    (.close wrtr)
    (let [table (wrap-dataset :text "test_assets/test.txt" "test_assets/test.txt.out" test-file-schema {:separator ","})]
      (is (= 2 (count (table-rows table)))))
    (when (exists? "test_assets/test.txt") (delete "test_assets/test.txt"))
    (when (exists? "test_assets/test.txt.out") (delete "test_assets/test.txt.out"))))
