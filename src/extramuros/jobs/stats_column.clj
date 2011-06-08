(ns ^{:doc "Jobs computing descriptive statistics of tables and table rows."
      :author "Antonio Garrote"}
  extramuros.jobs.stats-column
  (:use [extramuros hdfs]
        [extramuros.datasets :only [open-dataset table-numeric-rows
                                    table-ordered-columns]]
        [extramuros.datasets :only [table-obj-to-schema]]
        [extramuros.jobs core])
  (:use [incanter.charts :only [histogram]]
        [incanter.core :only [view]]))

;; Centrality measures job

(defmethod job-info :centrality-stats [_]
  "Computes the average of the values for a column in a table. It also returns the minimum, maximum and number of not null rows values.

   * Options:
     - output-path : path where the final computed values will be stored as a sequence file with keys: 'avg', 'min',  'max' and 'count'.
     - column      : name of the column whose statistics will be computed.
     - table       : table whose rows are going to be counted

   * Output path:
     Path to the file where the computed values are stored as a sequence file with keys: 'avg', 'min',  'max' and 'count'.

   * Output:
     A java HashMap with the computed values stored in keys: 'avg', 'min',  'max' and 'count'.

   * Visualization:
     Spreadsheet with the computed values.")

(defn- centrality-stats-column-job
  ([column table-or-path output-path]
     (let [table (table-from-table-or-path table-or-path)
           job (extramuros.java.jobs.stats.centrality.Job. column table output-path *conf*)]
       (.run job)
       job)))

(deftype CentralityStatsJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (centrality-stats-column-job
                                    (:column @configuration)
                                    (:table @configuration)
                                    (:output-path @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] [["average" (get (output this) "avg")]
                            ["min value" (get (output this) "min")]
                            ["max value" (get (output this) "max")]
                            ["not nulls" (get (output this) "count")]])
         (visualize [this options] (visualize this)))

(defmethod make-job :centrality-stats [id]
  (CentralityStatsJob. (atom nil) (atom nil)))

;; Dispersion measures job

(defmethod job-info :dispersion-stats [_]
  "Computes the variance and standard deviation for the values of a column in a table.

   * Note:
     This job requires the average value for the column rows in order to work. This value can be
     obtained with the centrality-stats job.

   * Options:
     - output-path : path where the final computed values will be stored as a sequence file with keys: 'var' and 'stdev'.
     - average     : average column value for the rows in the table
     - column      : name of the column whose statistics will be computed.
     - table       : table whose stats are going to be counted

   * Output path:
     Path to the file where the computed values are stored as a sequence file with keys: 'var' and 'stdev'.

   * Output:
     A java HashMap with the computed values stored in keys: 'var' and 'stdev'.

   * Visualization:
     Spreadsheet with the computed values.")


(defn- dispersion-stats-column-job
  ([column average table-or-path output-path]
     (let [table (table-from-table-or-path table-or-path)           
           job (extramuros.java.jobs.stats.dispersion.Job. column average table output-path *conf*)]
       (.run job)
       job)))

(deftype DispersionStatsJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (dispersion-stats-column-job
                                    (:column @configuration)
                                    (:average @configuration)
                                    (:table @configuration)
                                    (:output-path @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] [["variance" (get (output this) "var")]
                            ["standard dev." (get (output this) "stdev")]])
         (visualize [this options] (visualize this)))

(defmethod make-job :dispersion-stats [id]
  (DispersionStatsJob. (atom nil) (atom nil)))

;; frequency distribution job

(defmethod job-info :frequency-distribution [_]
  "Computes the distribution of frequencies for the values in a table column.

   * Options:
     - output-path : path where the final computed values will be stored as a sequence of pairs value/frequency.
     - column      : name of the column whose frequencies will be computed.
     - table       : table whose rows are going to be used.

   * Output path:
     Path to the file where the computed values are stored as a sequence file with pairs value/frequency.

   * Output:
     A sequence file iterator of Mahout Pairs value/frequency

   * Visualization:
     Histogram of frequencies")

(defn- frequency-distribution-column-job
  ([column table-or-path output-path]
     (let [table (table-from-table-or-path table-or-path)           
           job (extramuros.java.jobs.stats.freqdistribution.Job. column table output-path *conf*)]
       (.run job)
       job)))

(deftype FrequencyDistributionJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (frequency-distribution-column-job
                                    (:column @configuration)
                                    (:table @configuration)
                                    (:output-path @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (job-output @job))
         (output [this options] (job-output @job))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] (histogram
                            (reduce concat (map (fn [[v n]] (repeat n v))
                                                (job-output-pairs @job)))
                            :x-label "value"
                            :y-label "frequency"
                            :title (str "frequency distribution for column " (:column @configuration))))
         (visualize [this options] (visualize this)))

(defmethod make-job :frequency-distribution [id]
  (FrequencyDistributionJob. (atom nil) (atom nil)))


;; Stats per table

(defmethod job-info :table-stats [_]
  "Computes centraltity and dispersion stats for all the numeric columns in a table.

   * Options:
     - output-path : path where the temporary output for all the intermediate jobs will be stored
     - table       : table whose stats are going to be computed.

   * Output path:
     Path to the directory where all the intermediary data have been stored.

   * Output:
     A map with all the values stored using the name of column as a key.
     For each column an additional map is stored containing all the computed stats with keys:
       - average
       - min
       - max
       - variance
       - standard-deviation

   * Visualization:
     Spreadsheet with all the computed values.")

(defn- table-stats-job
  ([table-or-path output-directory]
     (let [table (table-map-from-table-map-or-path table-or-path)
           numeric-columns (table-numeric-rows table)
           output-directory (path output-directory)]
       ;; paths
       (println "checking directory")
       (when (exists? output-directory)
         (delete output-directory))
       (println "creating directory")
       (mkdir output-directory)
       (println (str "starting to compute stats for columns: " (vec numeric-columns)))
       (reduce (fn [ac [c m]] (assoc ac c m))
               {}
               (map (fn [column-name]
                      (let [centrality-job (make-job :centrality-stats)
                            dispersion-job (make-job :dispersion-stats)
                            ;frequencies-job (make-job :frequency-distribution)
                            ]
                        (println (str "STATS FOR COLUMN: " column-name))
                        ;; centrality
                        (set-config centrality-job
                                    {:column column-name
                                     :table table
                                     :output-path (path-to-string (suffix output-directory (str "/" column-name "/centrality")))})
                        (run centrality-job)
                        ;; dispersion
                        (set-config dispersion-job
                                    {:column column-name
                                     :average (get (output centrality-job) "avg")
                                     :table table
                                     :output-path (path-to-string (suffix output-directory (str "/" column-name "/dispersion")))})
                        (run dispersion-job)
                        ;; frequencies
                        ;;(set-config frequencies-job
                        ;;            {:column column-name
                        ;;             :table table
                        ;;             :output-path (path-to-string (suffix output-directory (str "/" column-name "/frequencies")))})
                        ;;(run frequencies-job)
                        
                        ;; output
                        [column-name
                         {:average            (get (output centrality-job) "avg")
                          :min                (get (output centrality-job) "min")
                          :max                (get (output centrality-job) "max")
                          :variance           (get (output dispersion-job) "var")
                          :standard-deviation (get (output dispersion-job) "stdev")
                          ;:frequencies        (job-output-pairs (output frequencies-job))
                          ;:frequencies-plot   (visualize frequencies-job)
                          }])) numeric-columns)))))


(deftype TableStatsJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (table-stats-job
                                    (:table @configuration)
                                    (:output-path @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] @job)
         (output [this options] (get @job options))
         (output-path [this] (:output-path @configuration))
         (output-path [this options] (suffix (:output-path @job) (str "/" options)))
         (visualize [this] ;(conj (map :frequencies-plot (vals @job))
                                 (cons ["column" "average" "min" "max" "variance" "standard dev"]
                                       (map (fn [[k stats]] [k (:average stats) (:min stats) (:max stats) (:variance stats) (:standard-deviation stats)]) @job)))
                           ;)
         (visualize [this options] ;(:frequencies-plot (get @job options))
                    (visualize this)))

(defmethod make-job :table-stats [id]
  (TableStatsJob. (atom nil) (atom nil)))

;; Normalization job

(defmethod job-info :normalization [_]
  "Normalizes the values in a table for some of its columns. The normalized values are stored in a new table where the rows are stored as a sequene file of Mahout Vectors.

   * Note:
     The normalized values are stored as double components in a Mahout vector. Numeric types in the
     old table will be casted to double.
     This job requires the min and max values for the columns to be normalized, these data can be obtained
     using the :centrality-stats or the :table-stats jobs.
     

   * Options:
     - output-path : path where the final computed values will be stored as a sequence file of vectors.
     - columns     : name of the columns whose statistics will be computed.
     - min-values  : a list with the ordered sequence of min. values for the columns to be normalized.
     - max-values  : a list with the ordered sequence of max. values for the columns to be normalized
     - table       : table whose rows are going to be normalized

   * Output path:
     Path of the directory where the normalized rows files are stored

   * Output:
     Table map containing the information of the new table for the normalized rows. The meta-data of the new table
     has not yet been written to the HDFS file system.

   * Visualization:
     None")


(defn- normalization-job
  ([columns min-values max-values output-path table-or-path]
     (let [table (table-from-table-or-path table-or-path)
           columns-array (let [a (make-array String (count columns))]
                           (loop [a a
                                  i 0
                                  columns columns]
                             (if (empty? columns)
                               a
                               (recur (do (aset a i (first columns)) a) (inc i) (rest columns)))))
           min-values-array (loop [a (double-array (count columns))
                                   i 0
                                   min-values min-values]
                              (if (empty? min-values)
                                a
                                (recur (do (aset a i (double (first min-values))) a) (inc i) (rest min-values))))
           max-values-array (loop [a (double-array (count columns))
                                   i 0
                                   max-values max-values]
                              (if (empty? max-values)
                                a
                                (recur (do (aset a i (double (first max-values))) a) (inc i) (rest max-values))))
           job (extramuros.java.jobs.stats.normalization.Job. columns-array min-values-array max-values-array table output-path *conf*)]
       (.run job)
       job)))

(deftype NormalizationJob [job configuration]  extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (normalization-job
                                    (:columns @configuration)
                                    (:min-values @configuration)
                                    (:max-values @configuration)                                    
                                    (:output-path @configuration)
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
         (output [this options] (output this))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] nil)
         (visualize [this options] nil))

(defmethod make-job :normalization [id]
  (NormalizationJob. (atom nil) (atom nil)))
