(use '[extramuros hdfs datasets])
(use '[extramuros.jobs core time-series])
(use 'extramuros.jobs.clustering.validation)
(use 'extramuros.visualization.core)
(use 'extramuros.visualization.3d)
(use 'extramuros.visualization.clustering)
(use 'incanter.core)

;; Connect to the Hadoop cluster
(bootstrap!  "HADOOP_HOME/conf/core-site.xml")

;; weather data

;; 1. date
;; 2. temperature in F.

(def *schema*
  (def-schema "date"           *date-time*	
              "temperature"    *double*))

;; importing the dataset into HDFS
(import-dataset "./examples/weather.data" "input/weather.data" *schema* :delim "," :date-formats {:date "yyyy-MM-dd"})

(def *table* (open-dataset "input/weather.data"))

; we filter the table to remove null rows
(def *filtered* (run-job :filter-table
                         {:directory-output "input/filtered_weather.data"
                          :table *table*
                          :filter-function (clojure (fn [row] (not (nil? (get row "temperature")))))}))
(write-table (output *filtered*))
(def *table* (output *filtered*))

;; plot time series

(def *sort* (run-job :time-series-sort
                     {:column "date"
                      :table *table*
                      :output-path "input/weather/sort"}))
(view (visualize *sort* "temperature"))

; max-min weekly
(def *aggregate* (run-job :time-series-aggregate
                           {:column "date"
                            :period :week
                            :aggregation-function :max
                            :table *table*
                            :output-path "input/weather/aggregate/week"}))
(view (visualize *aggregate* "temperature"))

(def *aggregate* (run-job :time-series-aggregate
                           {:column "date"
                            :period :week
                            :aggregation-function :min
                            :table *table*
                            :output-path "input/weather/aggregate/week"}))
(view (visualize *aggregate* "temperature"))


; aggregate yearly
(def *aggregate* (run-job :time-series-aggregate
                           {:column "date"
                            :period :year
                            :aggregation-function :average
                            :table *table*
                            :output-path "input/weather/aggregate/year"}))
(view (visualize *aggregate* "temperature"))


;; stationality

(def *stationality* (run-job :time-series-stationality
                             {:column "temperature"
                              :date-column "date"
                              :table *table*
                              :period :week
                              :output-path "input/weather/aggregate/week/seasonality"}))
(map view (visualize *stationality*))
