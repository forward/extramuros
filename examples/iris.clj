(use '[extramuros hdfs datasets])
(use '[extramuros.jobs core stats-column file clustering])
(use 'extramuros.jobs.clustering.validation)
(use 'extramuros.visualization.core)
(use 'extramuros.visualization.3d)
(use 'extramuros.visualization.clustering)
(use 'incanter.core)

;; Connect to the Hadoop cluster
(bootstrap!  "HADOOP_HOME/conf/core-site.xml")

;; schema for iris dataset

;; 1. sepal length in cm
;; 2. sepal width in cm
;; 3. petal length in cm
;; 4. petal width in cm
;; 5. class:
;;   -- Iris Setosa
;;   -- Iris Versicolour
;;   -- Iris Virginica

(def *schema*
  (def-schema "sepal_length"   *double*	
              "sepal_width"    *double*
              "petal_length"   *double*
              "petal_width"    *double*
              "class"          *categorical*))

;; importing the dataset into HDFS
(import-dataset "./examples/iris.data" "input/iris.data" *schema* :delim ",")

(def *table* (open-dataset "input/iris.data"))


;; computing statistics (centrality, dispersion) for all the numeric columns
(def *stats* (run-job :table-stats
                      {:output-path "input/iris/stats"
                       :table *table*}))

(view (visualize *stats*))

;; normalization of input data
(def *normalized* (run-job :normalization
                           {:output-path "input/iris/normalized"
                            :columns (table-numeric-columns *table*)
                            :min-values (map #(:min (output *stats* %))
                                             (table-numeric-columns *table*))
                            :max-values (map #(:max (output *stats* %))
                                             (table-numeric-columns *table*))
                            :table *table*}))

;; we save the normalized table
(write-table (output *normalized*))

(def *table* (open-dataset "input/iris/normalized.tbl.tbl"))

;; plot frequency distributions for the different dimensions
(doseq [column (table-numeric-columns *table*)]
  (let [*freqs* (run-job :frequency-distribution
                         {:output-path "input/iris/frequencies"
                          :column column
                          :table *table*})]
    (view (visualize *freqs*))))


;; Canopy + KMeans clustering
(def *kmeans* (let [canopy (run-job :canopy
                      {:output-path "input/iris/canopy"
                       :distance :euclidean
                       :t1 0.8
                       :t2 0.6
                       :should-cluster false
                       :table *table*})]
                (run-job :kmeans
                         {:output-path "input/iris/kmeans"
                          :input-clusters-path (output-path canopy :clusters)
                          :table *table*
                          :distance :euclidean
                          :convergence-delta 0.1
                          :num-iterations 10
                          :should-cluster true})))

;; check number of points per cluster
(map (fn [[k ps]] [k (count ps)])
     (output *kmeans* :folded-points))


;; we visualize the clusters in all the dimensions
(map view (visualize *kmeans*))


;; 3d plotting
(view-3d
 (plot-3d-clustering-output *kmeans* "sepal_length" "sepal_width" "petal_width")
 "some dimensions")

;; Davies-Bouldin index
(println (str "Validation index: "
              (output (run-job :davies-bouldin-index
                               {:clustering-job *kmeans*
                                :output-path "input/iris/davies-bouldin"}))))


;; Visualization examples

(view-table :box-plot *table* {:columns ["sepal_length" "sepal_width"]})

(view-table :scatter-plot *table* {:columns ["sepal_length" "sepal_width"]})

(view-table :scatter-plot-grouped *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

(view-table :frequencies-histogram *table* {:columns ["sepal_length" "sepal_width"]})

(view-table :scatter-3d *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

(view-table :parallel-lines *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

(view-table :parallel-lines *table* {:columns ["sepal_length" "sepal_width" "petal_width"] :sample 0.5})
