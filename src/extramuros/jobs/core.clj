(ns ^{:doc "Common functionality for all the defined jobs."
      :author "Antonio Garrote"}
  extramuros.jobs.core
  (:use [extramuros.datasets :only [parse-writable open-dataset]]))

(defn table-from-table-or-path
  "Returns a Table object from the provided table path string or Table map"
  ([table-or-path]
     (if (string? table-or-path) (table-from-table-or-path (open-dataset table-or-path))
         (if (map? table-or-path) (:table table-or-path)
             table-or-path))))

(defn table-map-from-table-map-or-path
  "Returns a Table map from the provided string or Table map"
  ([table-map-or-path]
     (if (string? table-map-or-path) (open-dataset table-map-or-path)
         table-map-or-path)))

;; Job object interface

(defn job-output
  "Output of a job as a Java object"
  ([job]
     (.getOutput job)))

(defn job-output-file
  "File/directory where this job has stored its output"
  ([job]
     (.getOutputFile job)))

(defn job-output-pairs
  "If the outptu of this job is a sequence file, returns an iterator over the stored pairs"
  ([job]
     (let [iterator (if (instance? java.util.Iterator job)
                      (iterator-seq job)
                      (iterator-seq (job-output job)))]
       (map (fn [pair] [(parse-writable (.getFirst pair)) (parse-writable (.getSecond pair))]) iterator))))

(defprotocol ExtramurosJob
  "A job that will run in the hadoop cluster"
  (run [this] "runs the job")
  (set-config [this map] "pass the configuration parameters")
  (config [this] "retrieves the configuration parameters")
  (job [this] "returns the java job object for this job")
  (output [this] [this options] "returns the output for this job")
  (output-path [this] [this options] "returns the output file for this job")
  (visualize [this] [this options] "tries to visualize the output for this job"))

(defmulti make-job identity)

(defmulti job-info identity)

(defn show-job-info
  ([job-name] (println (job-info job-name))))

(defn key-to-class-str
  "Transforms a Clojure keyword into the string with the name of the Java class it represents"
  ([key]
     (condp = key
         :gaussian  "org.apache.mahout.clustering.dirichlet.models.GaussianClusterDistribution"
         :dense     "org.apache.mahout.math.DenseVector"
         :sparse    "org.apache.mahout.math.DenseVector"
         :euclidean "org.apache.mahout.common.distance.EuclideanDistanceMeasure"
         :sparse    "org.apache.mahout.math.RandomAccessSparseVector"
         :sparse-random "org.apache.mahout.math.RandomAccessSparseVector"
         :sparse-sequential "org.apache.mahout.math.SequentialAccessSparseVector")))

(defn key-to-class
  "Transforms a Clojure keyword into the Java class it represents"  
  ([key]
     (Class/forName (key-to-class-str key))))

(defn run-job
  "Creates, configure and executes a job"
  ([job-name options]
     (let [job (make-job job-name)]
       (set-config job options)
       (run job)
       job)))

(defmacro clojure [& args]
  `(clojure-fn ~@args ~*ns*))

(defn clojure-fn [f ns]
  (let [ns-name (str ns "/")]
    (.replace (str f) ns-name "")))
