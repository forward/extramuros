(ns extramuros.jobs.clustering.validation
  (:use [extramuros.jobs core]
        [extramuros math]
        [extramuros.visualization clustering]
        [extramuros.hdfs :only [*conf* path pairs seq-file-reader]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Davies-Bouldin Index
(defn davies-bouldin-index-job
  ([clusters-path vectors-path output-path configuration]
     (let [job (extramuros.java.jobs.clustering.validation.daviesbouldin.Job.
                clusters-path
                vectors-path
                output-path
                configuration)]
       (.run job)
       job)))
