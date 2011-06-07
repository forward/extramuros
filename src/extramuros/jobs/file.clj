(ns extramuros.jobs.file
  (:use [extramuros.jobs core]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Count lines job

(defn count-lines-job
  ([output-file table-or-path]
     (let [job (extramuros.java.jobs.file.countlines.Job.
                (path output-file)
                (table-from-table-or-path table-or-path)
                *conf*)]
       (.run job)
       job)))

(deftype FileCountLinesJob [job configuration]  extramuros.jobs.core.ExtramurosJob
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

(defmethod make-job :file-count-lines [_]
  (FileCountLinesJob. (atom nil) (atom nil)))


;; Sample file job

(defn sample-file-job
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

(defn probabilistic-sample-table-job
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
         (output [this] (let [table (job-output @job)
                              _ (.setTablePath table (str (.getTablePath table) ".tbl"))]
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
