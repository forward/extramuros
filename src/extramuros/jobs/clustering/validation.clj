(ns extramuros.jobs.clustering.validation
  (:use [extramuros.jobs core]
        [extramuros math]
        [extramuros.visualization clustering]
        [extramuros.hdfs :only [*conf* path pairs seq-file-reader]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Davies-Bouldin Index

(defmethod job-info :davies-bouldin-index [_]
  "Davies-Bouldin Validity Index. It is a function of the ratio of the sum of within-cluster scatter to between-cluster separation.
   The better the clustering the lower the value of this index will be.

   * Options:
     - clustering-job:      An already executed clustering job that will be used to extract the values for the clusters-path
                            and the vectors path. The clustering job must have been executed with clustering.
     - clusters-path:       Path to the directory where the clusters have been generated. This parameter is not necessary if the
                            clustering-job option is set.
     - vectors-path         Path to the directory where the clustered points have been written. This parameter is not necessary
                            if the clustering-job option is set.
     - output-path:         path to the directory where the computed index will be stored as a sequence file with a single value
                            named '/index.seq'.

   * Output path:
     Path to sequence file where the computed metric will be stored as a sequence file with a single value.

   * Output:
     Value of the computed index.

   * Visualization:
     None.")

(defn davies-bouldin-index-job
  ([clusters-path vectors-path output-path configuration]
     (let [job (extramuros.java.jobs.clustering.validation.daviesbouldin.Job.
                clusters-path
                vectors-path
                output-path
                configuration)]
       (.run job)
       job)))

(deftype DaviesBouldinIndexJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (if (:clustering-job @configuration)
                                     (davies-bouldin-index-job
                                      (output-path (:clustering-job @configuration) :clusters)
                                      (output-path (:clustering-job @configuration) :clustered-points)
                                      (:output-path @configuration)
                                      *conf*)
                                     (davies-bouldin-index-job
                                      (:clusters-path @configuration)
                                      (:vectors-path @configuration)
                                      (:output-path @configuration)
                                      *conf*))]
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

(defmethod make-job :davies-bouldin-index [id]
  (DaviesBouldinIndexJob. (atom nil) (atom nil)))
