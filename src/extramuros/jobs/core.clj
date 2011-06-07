(ns extramuros.jobs.core
  (:use [extramuros.datasets :only [parse-writable open-dataset]]))

(defn table-from-table-or-path
  ([table-or-path]
     (if (string? table-or-path) (table-from-table-or-path (open-dataset table-or-path))
         (if (map? table-or-path) (:table table-or-path)
             table-or-path))))

(defn table-map-from-table-map-or-path
  ([table-map-or-path]
     (if (string? table-map-or-path) (open-dataset table-map-or-path)
         table-map-or-path)))

(defn job-output
  ([job]
     (.getOutput job)))

(defn job-output-file
  ([job]
     (.getOutputFile job)))

(defn job-output-pairs
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
