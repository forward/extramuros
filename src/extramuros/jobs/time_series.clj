(ns ^{:doc "Jobs to manipulate time series."
      :author "Antonio Garrote"}
  extramuros.jobs.time-series
  (:use [extramuros.jobs core]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema table-points-seq-for-column
                                    table-numeric-columns row-to-seq]]
        [extramuros.visualization.core])
  (:import [extramuros.java.jobs.timeseries.sort.Job]))

;; sort table
(defmethod job-info :time-series-sort [_]
  "Sorts a table using a date-time column as the key.

   * Options:
     - column :        the column of date-time type to be used to sort the table
     - output-path :   path where the sorted table will be written.
     - table :         the table to sort by date.

   * Output path:
     The path to the sorted table rows.

   * Output:
     A table map for the sorted table. The table meta data has not yet been written into the HDFS file system.

   * Visualization:
     - no arguments : Time series plot for all the numeric columns in the sorted table.
     - column-name  : Time series plot for the column passed as a parameter")


(defn- time-series-sort-job
  ([column-name table output-path]
     (let [job (extramuros.java.jobs.timeseries.sort.Job.
                column-name
                table
                output-path
                *conf*)]
       (.run job)
       job)))

(deftype TimeSeriesSortJob [job configuration] extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (time-series-sort-job
                                    (:column @configuration)
                                    (:table (:table @configuration))
                                    (:output-path @configuration))]
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
         (visualize [this] (let [dates (table-points-seq-for-column (output this) (:column @configuration))]
                             (map (fn [column]
                                    (incanter.charts/line-chart
                                     dates
                                     (table-points-seq-for-column (output this) column)
                                     :x-label (:column @configuration)
                                     :y-label column
                                     :title (str "time series - " column)))
                                  (table-numeric-columns (job-output @job)))))
         (visualize [this options] (let [dates (table-points-seq-for-column (output this) (:column @configuration))]
                                     (incanter.charts/line-chart
                                      dates
                                      (table-points-seq-for-column (output this) options)
                                      :x-label (:column @configuration)
                                      :y-label options
                                      :title (str "time series - " options)))))

(defmethod make-job :time-series-sort [_]
  (TimeSeriesSortJob. (atom nil) (atom nil)))


;; aggregate time series

(defmethod job-info :time-series-aggregate [_]
  "Aggregates the data in a table using based in one date-time column and the period passed as an argument. Different aggreagation
   functions can be passed as parameters.

   * Notice:
     This job creates a new table storing the data in rows although the initial table is a wrapped table.

   * Options:
     - column :               the column of date-time type to be used to sort the table
     - period :               the period that will be used to aggregate the dates in the rows.
                              Possible options are:
                              - year  : aggregate yearly
                              - month : aggregate monthly
                              - week  : aggregate weekly
                              - day   : aggregate daily
     - aggregation-function : how to aggregate values in the same time period.
                              Possible options are:
                              - average : the average for the period is computed
                              - max : the max value for the period is chosen
                              - min : the min value for the period is chosen
                              - sum : the sum for the values in the period is computed
     - output-path :          path where the sorted table will be written.
     - table :                the table to sort by date.

   * Output path:
     The path to the aggregated table rows.

   * Output:
     A table map for the aggregated table. The table meta data has not yet been written into the HDFS file system.

   * Visualization:
     - no arguments : Time series plot for all the numeric columns in the sorted table.
     - column-name  : Time series plot for the column passed as a parameter")

(defn time-series-aggregate-job
  ([column-name period aggregation-function table output-path]
     (let [job (extramuros.java.jobs.timeseries.aggregate.Job.
                column-name
                period
                aggregation-function
                table
                output-path
                *conf*)]
       (.run job)
       job)))

(deftype TimeSeriesAggregateJob [job configuration] extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (time-series-aggregate-job
                                    (:column @configuration)
                                    (name (:period @configuration))
                                    (name (:aggregation-function @configuration))
                                    (:table (:table @configuration))
                                    (:output-path @configuration))]
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
         (visualize [this] (let [dates (table-points-seq-for-column (output this) (:column @configuration))]
                             (map (fn [column]
                                    (incanter.charts/line-chart
                                     dates
                                     (table-points-seq-for-column (output this) column)
                                     :x-label (:column @configuration)
                                     :y-label column
                                     :title (str "time series - " column " - period " (name (:period @configuration)))))
                                  (table-numeric-columns (job-output @job)))))
         (visualize [this options] (let [dates (table-points-seq-for-column (output this) (:column @configuration))]
                                     (incanter.charts/line-chart
                                      dates
                                      (table-points-seq-for-column (output this) options)
                                      :x-label (:column @configuration)
                                      :y-label options
                                      :title (str "time series - " options " - period " (name (:period @configuration)))))))

(defmethod make-job :time-series-aggregate [_]
  (TimeSeriesAggregateJob. (atom nil) (atom nil)))

;; stationality

(defmethod job-info :time-series-stationality [_]
  "This funtion detects stationality in a time series computing the average evolution of the average and the variance for the data in the
   table along time.

   * Notice:
     This job creates a new table storing the data in rows although the initial table is a wrapped table.

   * Options:
     - column  :      the column whose average and variance evolution will be computed.
     - date-column  : the column of date-time type to be used to sort the table
     - period :       the period that will be used to aggregate the dates in the rows.
                      Possible options are:
                      - year  : aggregate yearly
                      - month : aggregate monthly
                      - week  : aggregate weekly
                      - day   : aggregate daily
     - output-path :  path where the sorted table will be written.
     - table :        the table to sort by date.

   * Output path:
     The path to the aggregated table rows.

   * Output:
     A map with the following results:
     - table   : a table containing the evolution for the average, variance, min and max value of the column passed as an argument.
     - avg-var : variance of the average across all the periods.
     - var-var : variance of the variance across all the perios.

   * Visualization:
     Plots for the evolution of the average, variance and a table showing the variance of these values.")

(defn time-series-stationality-job
  ([column-name date-column period table output-path]
     (let [job (extramuros.java.jobs.timeseries.stationality.Job.
                column-name
                date-column
                period
                table
                output-path
                *conf*)]
       (.run job)
       job)))

(deftype TimeSeriesStationalityJob [job configuration] extramuros.jobs.core.ExtramurosJob
         (run [this] (let [job-run (time-series-stationality-job
                                    (:column @configuration)
                                    (:date-column @configuration)
                                    (name (:period @configuration))
                                    (:table (:table @configuration))
                                    (:output-path @configuration))]
                       (swap! job (fn [_] job-run))))
         (set-config [this map] (swap! configuration (fn [_] map)))
         (config [this] @config)
         (job [this] @job)
         (output [this] (let [table (job-output @job)
                              _ (.setTablePath table (str (.getTablePath table) ".tbl"))]
                          {:table {:table table
                                   :path (.getTablePath table)
                                   :schema (table-obj-to-schema table)}
                           :avg-var (incanter.stats/variance (map (fn [row] (nth (row-to-seq row) 1)) (iterator-seq (.iterator table))))
                           :var-var (incanter.stats/variance (map (fn [row] (nth (row-to-seq row) 2)) (iterator-seq (.iterator table))))}))
         (output [this options] (output this))
         (output-path [this] (job-output-file @job))
         (output-path [this options] (job-output-file @job))
         (visualize [this] (let [output-job (output this)
                                 dates (table-points-seq-for-column (:table output-job) "date")]
                             (conj
                              (map (fn [column]
                                     (incanter.charts/line-chart
                                      dates
                                      (table-points-seq-for-column (:table output-job) column)
                                      :x-label "date"
                                      :y-label column
                                      :title (str "time series - " column " - period " (name (:period @configuration)))))
                                   ["average" "variance"])
                              [["averages variance" (:avg-var output-job)]
                               ["variances variance" (:var-var output-job)]])))
         (visualize [this options] (visualize this)))

(defmethod make-job :time-series-stationality [_]
  (TimeSeriesStationalityJob. (atom nil) (atom nil)))
