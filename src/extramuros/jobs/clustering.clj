(ns extramuros.jobs.clustering
  (:use [extramuros.jobs core]
        [extramuros math]
        [extramuros.visualization clustering]
        [extramuros.hdfs :only [*conf* path pairs seq-file-reader]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Canopy adapter

(defn canopy-adapter-job
  ([output-file table-or-path distance t1 t2 run-clustering configuration]
     (let [job (extramuros.java.jobs.clustering.canopy.Job.
                output-file
                (table-from-table-or-path table-or-path)
                distance
                (double t1)
                (double t2)
                (boolean run-clustering)
                *conf*)]
       (.run job)
       job)))

(deftype CanopyAdapterJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (canopy-adapter-job
                                    (:output-path @configuration)
                                    (:table (:table @configuration))
                                    (build-distance (or (:distance @configuration) :euclidean))
                                    (:t1 @configuration)
                                    (:t2 @configuration)
                                    (:should-cluster @configuration)
                                    *conf*)]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (let [output (job-output @job)]
                                  (condp = options
                                      :clusters (reduce (fn [ac c] (assoc ac (str (cluster-id c)) c))
                                                        {}
                                                        (iterator-seq (get output "cluster-iterator")))
                                      :folded-points (reduce (fn [acum p]
                                                               (let [label (.getFirst p)
                                                                     vector (.getSecond p)
                                                                     points (get acum label)]
                                                                 (if (nil? points)
                                                                   (assoc acum label [vector])
                                                                   (assoc acum label (conj points vector)))))
                                                             {}
                                                             (iterator-seq (get output "points-iterator")))
                                      :clustered-points-table (let [tbl (get output "clustered-points")]
                                                                {:schema (table-obj-to-schema tbl)
                                                                 :path (.getTablePath tbl)
                                                                 :table tbl}))))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (condp = options
                                         :clusters (get (output this) "clusters")
                                         :clustered-points (.getRowsPath (get (output this) "clustered-points"))))
         (visualize [this] (plot-clustering-output this true))
         (visualize [this options] (plot-clustering-output this (:x-label options) (:y-label options) options)))

(defmethod make-job :canopy [id]
  (CanopyAdapterJob. (atom nil) (atom nil)))
