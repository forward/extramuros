#Extramuros

Extramuros is a library that makes easier to work with machine learning algorithms in Hadoop. It is built on top of Apache Mahout and uses Clojure as a convenient scripting language.
At this moment the library only covers clustering algorithms and some other statistical utilities.

##Installation

You can use leiningen to get the basic dependencies required by the library:
    $lein deps

Nevertheless, some dependencies must be installed by hand:

- Hadoop:  the library has been tested with Apache Hadoop 0.20 and Clouder Hadoop distribution 3 beta 4.
- Mahout:  the library has been tested with Mahout 0.5
- Clojure: clojure 1.2 and clojure.contrib are required to execute clojure expressions in the Hadoop jobs.
- jzy3d:   is used by the visualization code: (http://code.google.com/p/jzy3d/), the java library and the native library must be present in the path.
                                                 
                                                 
The java components of the library must be available for the nodes of the Hadoop cluster as well as in the Clojure classpath. They can be found in the ExtramurosJava.jar file.


##A classic example
This is an example of how to use the library to preprocess, cluster and visualize the results in a clustering problem. As an example toy dataset we will use the well known [iris flower dataset](http://en.wikipedia.org/wiki/Iris_flower_data_set).
The script and input data can be found in the example directory of the project.

### Bootstraping the library

In order to use library you must first connect to Hadoop cluster and HDFS file system. You can the function *bootstrap* to acomplish this task. You can pass as an argument a list of Hadoop configuration files.

    ;; Connect to the Hadoop cluster
    (bootstrap!  "HADOOP_HOME/conf/core-site.xml")

### Importing a dataset

Extramuros works with datasets defined as tables with a header and data rows. Columns in this tables have an associated name and type. Cells in the rows must match the provided type or be null.
Tables can be created importing text files from the local file or can wrap HDFS files or directories. At this moment the library supports text files or sequence files. Take a look at the *extramuros.datasets/wrap-dataset* for more information.
In this example we are going to import the iris flower data from the local file system so we define an schema to describe the columns of the dataset and import the data using the *extramuros.dataset/import-dataset* function:

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
    (import-dataset "./examples/iris.data" "input/iris.data" *schema* 
                    :delim "," :nulls [""])

    (def *table* (open-dataset "input/iris.data"))

### Running jobs
Most of the functionality of the library is implemented in a collection of jobs that perform certain computations on tables.
Jobs are implemented in Java classes and they are wrapped by a Clojure type. All the job wrappers conform to the same protocol defining functions to configure, execute, retrieve the job output, retrieve the job output paths and the visualization of the job.
There is a helper *extramuros.jobs.core/run-job* function that can be used to create, configure and run a job in a single function call.

In the following example basic stats for the iris table are computed and then visualized. The *view* function is defined in the Incanter library

    ;; computing statistics (centrality, dispersion) for all the numeric columns
    (def *stats* (run-job :table-stats
                          {:output-path "input/iris/stats"
                           :table *table*}))
     
    (view (visualize *stats*))

<img src='https://github.com/forward/extramuros/raw/master/readme_files/stats.png'></img>

The output of this command should be a table showing the main statistics for the table


The following job normalizes the data using the min and max values for each column computed in the previous task. The output is written in a new table using the *extramuros.datasets/write-table* function.
The resulting table is a projection of the selected columns in the normalization job.

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



After normalizing we plot the frequency distribution for the normalized values. We use again the Incanter *view* function to display the charts:

    ;; plot frequency distributions for the different dimensions
    (doseq [column (table-numeric-columns *table*)]
      (let [*freqs* (run-job :frequency-distribution
                             {:output-path "input/iris/frequencies"
                              :column column
                              :table *table*})]
        (view (visualize *freqs*))))

<img src='https://github.com/forward/extramuros/raw/master/readme_files/freqs.png'></img>     

At this point we run the clustering algorithm chaining the execution of the canopy Mahout clustering algorithm with the Mahout implementation of KMeans.
After the clustering we use Clojure to print the number of points assigned to each cluster

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
    (println  (map (fn [[k ps]] [k (count ps)])
               (output *kmeans* :folded-points)))

We can plot the relationship between the columns of the clustered points using the *visualize* function of the job interface and the Incanter *view* function in the same way as we did with the previous jobs

    ;; we visualize the clusters in all the dimensions
    (map view (visualize *kmeans*))

<img src='https://github.com/forward/extramuros/raw/master/readme_files/clusters3d.png'></img>

It is also possible to explore the relationship between thre dimensions of the clustered data using the *extramuros.visualization.clustering/plot-3d-clustering-output* function and the *extramuros.visualization.3d/view-3d* function:

    ;; 3d plotting
    (view-3d
     (plot-3d-clustering-output *kmeans* "sepal_length" "sepal_width" "petal_width")
     "some dimensions")

<img src='https://github.com/forward/extramuros/raw/master/readme_files/clusters3d.png'></img>

Finally, we can compute some information about the quality of the clusters computing the Davies-Bouldin index.

    ;; Davies-Bouldin index
    (println (str "Validation index: "
                  (output (run-job :davies-bouldin-index
                                   {:clustering-job *kmeans*
                                    :output-path "input/iris/davies-bouldin"}))))

##Working with time series

At the present moment, the library includes some limited support to work with time series.
An example of time series analysis is included in the *examples/weather.clj* file.

The example is an analysis of some weather data through three different years. The schema for these data is the following:

    ;; weather data
     
    ;; 1. date
    ;; 2. temperature in F.
     
    (def *schema*
      (def-schema "date"           *date-time*	
                  "temperature"    *double*))

The *date* column is marked as having **date-time** type. In order to work with dates it is mandatory to provide a way to parse
dates from their textual representation. This can be accomplished adding an additional map with the formats when importing or
wrapping a dataset.

    ;; importing the dataset into HDFS
    (import-dataset "./examples/weather.data" "input/weather.data" *schema* :delim "," :date-formats {:date "yyyy-MM-dd"})
     
    (def *table* (open-dataset "input/weather.data"))

Current implementation of time series jobs does not support null values. The following code remove all rows with null values from
the table:

    ; we filter the table to remove null rows
    (def *filtered* (run-job :filter-table
                             {:directory-output "input/filtered_weather.data"
                              :table *table*
     
                              :filter-function (clojure `(fn [row] (not (nil? (get row "temperature")))))}))
    (write-table (output *filtered*))
    (def *table* (output *filtered*))

If the data in the table is not sorted, the following job will sort the rows in chronological order according to a date-time column

    ;; plot time series
     
    (def *sort* (run-job :time-series-sort
                         {:column "date"
                          :table *table*
                          :output-path "input/weather/sort"}))
    (view (visualize *sort* "temperature"))

The visualization of this job will plot the sorted time series.

<img src='https://github.com/forward/extramuros/raw/master/readme_files/timeseries1.png'></img>

The *time-series-aggregate* job can be used to aggregate data in the time series for a period of time: day, week, month, year...
The *aggregation-function* key indicates how the aggregation will be made: sum, average, max, min, etc.
In the example the time series is aggregated weekly using the max and min values:

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

<img src='https://github.com/forward/extramuros/raw/master/readme_files/timeseries2.png'></img>

Data is also aggregated yearly using the average function:

    ; aggregate yearly
    (def *aggregate* (run-job :time-series-aggregate
                               {:column "date"
                                :period :year
                                :aggregation-function :average
                                :table *table*
                                :output-path "input/weather/aggregate/year"}))
    (view (visualize *aggregate* "temperature"))

<img src='https://github.com/forward/extramuros/raw/master/readme_files/timeseries3.png'></img>

Some stationality analysis can be performed in the data using the *time-series-stationality* job that computes the average and variance for the data in the time series in the indicated period of time:

    ;; stationality
     
    (def *stationality* (run-job :time-series-stationality
                                 {:column "temperature"
                                  :date-column "date"
                                  :table *table*
                                  :period :week
                                  :output-path "input/weather/aggregate/week/seasonality"}))
    (map view (visualize *stationality*))

<img src='https://github.com/forward/extramuros/raw/master/readme_files/timeseries4.png'></img>

##Table visualization

The library implements a collection of functions to visualize the data stored in HDFS tables. 
The functions receive the kind of visualization that must be plotted, the table to plot and a map with options.

All the jobs can receive a *:sample* option with a sampling percentage. If this parameter is passed, the rows in the table will be sampled using the *probabilistic-sample-table* job and the sampled data will be the basis of the visualization.
The sampled data will be stored in a temporal directory in the HDFS file system.

### box plot

Options:

- title
- legend
- y-label
- x-label
- series-label
- columns

Example code:

    (view-table :box-plot *table* {:columns ["sepal_length" "sepal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/box_plot.png'></img>

### scatter plot

Options:

- title
- legend
- columns

Example code:

    (view-table :box-plot *table* {:columns ["sepal_length" "sepal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/scatter_plot.png'></img>

### grouped scatter plot

Options:

- title
- legend
- columns

Example code:

    (view-table :scatter-plot-grouped *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/scatter_plot_grouped.png'></img>

### frequencies histogram

Options:

- title
- x-label
- y-label
- columns

Example code:

    (view-table :frequencies-histogram *table* {:columns ["sepal_length" "sepal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/frequencies.png'></img>

### scatter 3d

Options:

- columns

Example code:

    (view-table :scatter-3d *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/scatter_3d.png'></img>

### parallel lines

Options:

- columns

Example code:

    (view-table :parallel-lines *table* {:columns ["sepal_length" "sepal_width" "petal_width"]})

<img src='https://github.com/forward/extramuros/raw/master/readme_files/parallel.png'></img>

##Supported types

At the present moment tables and wrapped tables support the following types in their definitions:

- string
- float
- double
- integer
- long
- categorical
- date-time
- null

The null type is used internally by the library to values in rows where a NULL value has been detected.
These types are defined in the interface *extramuros.java.formats.RowTypes*. 

Some wrapped tables does not support all the data types. For example, a wrapped vector sequence file will only support numeric data types and dates that can be converted into double values.

###Working with dates

Columns in a table can be declared to be of type *date-time*. In this case, an additional map containing format strings for that column must be specified when the table is imported/wrapped.
The format information will be used to transform the literal representation of the date into a *java.util.Date* object.

    (let [test-file-schema (def-schema :name *string* :columna *integer* :columnb *date-time*)]
      (wrap-dataset :text "test_assets/test.txt" "test_assets/test.txt.out" test-file-schema 
                    {:separator "," :date-formats {:columnb "dd-mm-yyyy"}}))

In the case of a table wrapping a vector sequence file, the date format specification is not required, since the value of the date will be stored as a long value.

##Implemented jobs

This is a list of all the jobs currently included in the library. You can get all the information about how to use a job using the *extramuros.jobs.core/job-info* function.

Statistics (extramuros.jobs.stats_column)

- centrality-stats
- dispersion-stats
- frequency-distribution
- table-stats
- normalization

Table manipulation (extramuros.jobs.file)

- table-count-lines
- sample-file
- probabilistic-sample-table
- vectorize-table
- filter-table

Clustering (extramuros.jobs.clustering)

- canopy
- kmeans
- proclus
- dirichlet

Validation (extramuros.jobs.clustering.validation)

- davies-bouldin-index

Time series (extramuros.jobs.time-series)

- time-series-sort
- time-series-aggregate
