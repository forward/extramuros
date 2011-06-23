(ns ^{:doc "Jobs to manipulate tables and other kind of files in a hadoop cluster."
      :author "Antonio Garrote"}
  extramuros.jobs.file
  (:use [extramuros.jobs core]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Count lines job

(defmethod job-info :table-count-lines [_]
  "Count the number of rows in a table in a distributed fashion.

   * Options:
     - output-path : path where the final count of lines will be stored in a sequence file with a single pair key/value
     - table :       table whose rows are going to be conted

   * Output path:
     Path to the file where the number of lines is stored as the single pair in a sequence file

   * Output:
     The number of lines as an integer

   * Visualization:
     None.")

(defn- count-lines-job
  ([output-file table-or-path]
     (let [job (extramuros.java.jobs.file.countlines.Job.
                (path output-file)
                (table-from-table-or-path table-or-path)
                *conf*)]
       (.run job)
       job)))

(deftype TableCountLinesJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (count-lines-job (:output-path @configuration)
                                                    (:table @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :table-count-lines [_]
  (TableCountLinesJob. (atom nil) (atom nil)))


;; Sample file job

(defmethod job-info :sample-file [_]
  "Sample a number of files expressed as a percentage of the total number of lines from a SequenceFile.
   The lines will be extracted randomly from the original file.

   * Notice:
     This job does not run distributed.
     The whole file will be scanned.
     This Job does not work with tables/table-adapters, only with sequence files.

   * Options:
     - file-to-sample:       path to the sequence file to sample. It must be a single file, directories are not supported.
     - file-output:          path to the file where sequence file with the sampled pairs will be written
     - total-lines:          total number of lines in the file.
     - percentage-to-sample: the percentage of lines that will be sampled as a value in the closed interval [0,1].

   * Output path:
     The path to the sequence file where the sampled lines are stored.

   * Output:
     An iterator for the values in the sampled sequence file.

   * Visualization:
     None.")

(defn- sample-file-job
  ([file-to-sample file-output total-lines percentage-to-sample]
     (let [job (extramuros.java.jobs.file.sample.Job. file-to-sample file-output (int total-lines) (float percentage-to-sample) *conf*)]
       (.run job)
       job)))

(deftype SampleFileJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (sample-file-job (:file-to-sample @configuration)
                                                    (:file-output @configuration)
                                                    (:total-lines @configuration)
                                                    (:percentage-to-sample @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :sample-file [_]
  (SampleFileJob. (atom nil) (atom nil)))


;; Probabilistic sampling table job

(defmethod job-info :probabilistic-sample-table [_]
  "Sample the rows of a table in a distributed way. A probabilitiy in the interval [0,1] for a row to be
   sampled must be provided.
   The implementation uses a uniform probability distribution to decide if a row is finally sampled.

   * Note:
     The job implementation will run an additional combiner job to combinne all the outputs from the sample
     job in a single file in the HDFS file system.

   * Options:
     - directory-output     : directory where the output for this job will be stored.
     - sampling-probability : probability that a row in the table will be copied to the sample table
     - table:               : table to be sampled

   * Output Path:
     Path to the HDFS file containing the sample rows.

   * Output:
     Returns a new Table map for sampled rows. The table metadata has not yet been written to disk.

   * Visualization:
     None.")

(defn- probabilistic-sample-table-job
  ([directory-output sampling-probability table]
     (let [job (extramuros.java.jobs.file.probabilisticsample.Job. directory-output
                                                                   (Double. (double sampling-probability))
                                                                   table
                                                                   *conf*)]
       (.run job)
       job)))

(deftype ProbabilisticSampleTableJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (probabilistic-sample-table-job
                                    (:directory-output @configuration)
                                    (:sampling-probability @configuration)
                                    (:table (:table @configuration)))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (let [table (job-output @job)]
                          (.setTablePath table (str (.getTablePath table) ".tbl"))
                          (.setConfiguration table *conf*)
                          {:table table
                           :path (.getTablePath table)
                           :schema (table-obj-to-schema table)}))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :probabilistic-sample-table [_]
  (ProbabilisticSampleTableJob. (atom nil) (atom nil)))


;; vectorization of tables

(defmethod job-info :vectorize-table [_]
  "Transforms a table into a new table whose rows are stored as Mahout vectors stored in a sequence file in
   HDFS file system
   A list with the columns to be transformed into the vector components must be specified.

   * Note:
     Only numeric columns can be vectorized.
     Rows containing null values for the selected columns will not be written in the output file.

   * Options:
     - directory-output     : directory where the output for this job will be stored.
     - column-names         : list of the columns to be used to extract the values for the vectors.
     - vector-type          : the type of Mahout vector that will be created, specified as a keyword.
                              Possible values are:
                              - dense                -> org.apache.mahout.math.DenseVector
                              - sequential-sparse    -> org.apache.mahout.math.SequentialAccessSparseVector
                              - random-access-sparse -> org.apache.mahout.math.RandomAccessSparseVector
                              - sparse               -> org.apache.mahout.math.RandomAccessSparseVector
     - table:               : table to be sampled

   * Output Path:
     Path to the HDFS file containing the vetorized rows.

   * Output:
     Returns a new Table map for vectorized rows. The table metadata has not yet been written to disk.

   * Visualization:
     None.")

(defn vectorize-table-job
  ([directory-output column-names vector-type table]
     (let [columns-array (loop [array (make-array String (count column-names))
                                idx 0
                                column-names column-names]
                           (if (empty? column-names)
                             array
                             (do (aset array idx (first column-names))
                                 (recur array (inc idx) (rest column-names)))))
           vector-class (condp = vector-type
                            :dense org.apache.mahout.math.DenseVector
                            :sequential-sparse org.apache.mahout.math.SequentialAccessSparseVector
                            :random-access-sparse org.apache.mahout.math.RandomAccessSparseVector
                            :sparse org.apache.mahout.math.RandomAccessSparseVector
                            (throw (Exception. (str "Unknown type of vector " vector-type))))
           job (extramuros.java.jobs.file.vectorize.Job. directory-output
                                                         columns-array
                                                         vector-class
                                                         table
                                                         *conf*)]
       (.run job)
       job)))

(deftype VectorizeTableJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (vectorize-table-job
                                    (:directory-output @configuration)
                                    (:column-names @configuration)
                                    (:vector-type  @configuration)
                                    (:table (:table @configuration)))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (let [table (job-output @job)
                              _ (.setTablePath table (str (.getTablePath table) ".tbl"))]
                          {:table table
                           :path (.getTablePath table)
                           :schema (table-obj-to-schema table)}))
         (output [this options] (let [to-return (job-output @job)]
                                  (.setConfiguration (:table to-return) *conf*)
                                  to-return))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :vectorize-table [_]
  (VectorizeTableJob. (atom nil) (atom nil)))

;; filter

(defmethod job-info :filter-table [_]
  "Filter the rows in a table stored in the HDFS file system using the provided predicate.

   * Note:
     Is possible to replicate this job from pure Java code extending the abstract AbstractFilterMapper
     class and implementing the abstract filter method.

   * Options:
     - directory-output     : directory where the output for this job will be stored.
     - filter-function      : a clojure expression that will be used to decide if the rows in the table will
                              be filtered.
                              The filter function must consist in a lambda function that receives a single parameter,
                              the row to be filtered, and returning true if the row must be written in the ourput
                              or false if the row must be removed.
                              The macro *clojure* can be used to build a filter using variables in the
                              closure where the invokation takes place.
                              e.g:
                              (let [min-val 1
                                    max-val 100]
                                (clojure `(fn [row] (and (> (get row \"val\") ~min-val)
                                                        (< (get row \"val\") ~max-val)))))
     - table:               : table to be filtered

   * Output Path:
     Path to the HDFS file containing the filtered rows.

   * Output:
     Returns a new Table map for the filtered rows. The table metadata has not yet been written to disk.

   * Visualization:
     None.")

(defn filter-clojure-job
  ([directory-output function-str table]
     (let [job (extramuros.java.jobs.file.filter.Job. (path directory-output)
                                                      table
                                                      extramuros.java.jobs.file.filter.ClojureFilterMapper
                                                      function-str
                                                      *conf*)]
       (.run job)
       job)))

(deftype FilterTableJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (filter-clojure-job
                                    (:directory-output @configuration)
                                    (:filter-function @configuration)
                                    (:table (:table @configuration)))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (let [table (job-output @job)]
                          (.setConfiguration table *conf*)
                          {:table table
                           :path (.getTablePath table)
                           :schema (table-obj-to-schema table)}))
         (output [this options] (let [to-return (job-output @job)]
                                  to-return))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :filter-table [_]
  (FilterTableJob. (atom nil) (atom nil)))
