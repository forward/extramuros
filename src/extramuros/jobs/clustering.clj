(ns extramuros.jobs.clustering
  (:use [extramuros.jobs core]
        [extramuros math]
        [extramuros.visualization clustering]
        [extramuros.hdfs :only [*conf* path pairs seq-file-reader]]
        [extramuros.datasets :only [table-obj-to-schema]]))

;; Canopy adapter

(defmethod job-info :canopy [_]
  "Canopy clustering algorithm.

   * Notice:
     - This job is just a wrapper around the Mahout implementation of the algorithm

   * Options:
     - output-path :   path where the clusters and optionally the clustered input vectors will be written.
     - table :         table with only numeric rows that will be used to generate the input vectors for the algorithm.
     - distance:       distance metric to measure distances between vectors. Possible values are:
                         + euclidean: Euclidean distance metric.
     - t1:             t1 canopy clustering algorithm parameter.
     - t2:             t2 canopy clustering algorithm parameter.
     - should-cluster: indicates if the clustering of the input vectors using the computed clusters must be executed.

   * Output path:
     Paths to the different computed outputs
     - no arguments:     path to the base output directory.
     - clusters:         path to the directory where the clusters have been written.
     - clustered-points: path to the directory where the clustered points have been written if should-cluster is true.

   * Output:
     - no arguments:           java HashMap with the computed outputs of the algorithm with keys:
                                 + clustered-points: A newly created table for the clustered points.
                                 + clusters: Path to the directory where the clusters have been written.
                                 + cluster-iterator: An iterator over the computed clusters.
                                 + points-iterator: An iterator over the clustered points if should-cluster is true.
     - clusters:               map of cluster-ids -> cluster-object for the computed clusters
     - folded-points:          map of cluster-ids -> sequence of vectors for all the clustered points if should-cluster is true.
     - clustered-points-table: table map for the newly created clustered points table.

   * Visualization:
     None.")

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
                                    configuration)]
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


;; KMeans adapter

(defmethod job-info :kmeans [_]
  "k-means clustering algorithm.

   * Notice:
     - This job is just a wrapper around the Mahout implementation of the algorithm

   * Options:
     - output-path :        path where the clusters and optionally the clustered input vectors will be written.
     - table :              table with only numeric rows that will be used to generate the input vectors for the algorithm.
     - input-clusters-path: path to a directory containing a single sequence file containing Mahout clusters that
                            will be used as the initial centroids in the k-means algorithm.
                            The file containing the clusters must be named 'part-r-00000'.
     - distance:            distance metric to measure distances between vectors. Possible values are:
                              + euclidean: Euclidean distance metric.
     - convergence-delta    double value with the minimum value required to stop the computation of the k-means algorithm
     - num-iterations       maximum number of iterations the algorithm will run if the convergence-delta is not found before.
     - should-cluster:      indicates if the clustering of the input vectors using the computed clusters must be executed.

   * Output path:
     Paths to the different computed outputs
     - no arguments:     path to the base output directory.
     - clusters:         path to the directory where the clusters have been written.
     - clustered-points: path to the directory where the clustered points have been written if should-cluster is true.

   * Output:
     - no arguments:           java HashMap with the computed outputs of the algorithm with keys:
                                 + clustered-points: A newly created table for the clustered points.
                                 + clusters: Path to the directory where the clusters have been written.
                                 + cluster-iterator: An iterator over the computed clusters.
                                 + points-iterator: An iterator over the clustered points if should-cluster is true.
     - clusters:               map of cluster-ids -> cluster-object for the computed clusters
     - folded-points:          map of cluster-ids -> sequence of vectors for all the clustered points if should-cluster is true.
     - clustered-points-table: table map for the newly created clustered points table.

   * Visualization:
     None.")

(defn kmeans-adapter-job
  ([output-path input-clusters-path table-or-path distance convergence-delta num-iterations run-clustering configuration]
     (let [job (extramuros.java.jobs.clustering.kmeans.Job.
                output-path
                input-clusters-path
                (table-from-table-or-path table-or-path)
                distance
                (double convergence-delta)
                (int num-iterations)
                (boolean run-clustering)
                configuration)]
       (.run job)
       job)))

(deftype KMeansAdapterJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (kmeans-adapter-job
                                    (:output-path @configuration)
                                    (:input-clusters-path @configuration)                                    
                                    (:table (:table @configuration))
                                    (build-distance (or (:distance @configuration) :euclidean))
                                    (:convergence-delta @configuration)
                                    (:num-iterations @configuration)
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

(defmethod make-job :kmeans [id]
  (KMeansAdapterJob. (atom nil) (atom nil)))
