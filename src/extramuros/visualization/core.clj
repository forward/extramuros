(ns ^{:doc "Graphical plotting primitives"
      :author "Antonio Garrote"}
  extramuros.visualization.core
  (:use [incanter core charts])
  (:use [extramuros.jobs core file]
        [extramuros math]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema table-column-position row-to-seq
                                    table-rows table-rows-file table-numeric-columns
                                    table-points-seq-for-column]])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable] 
           [org.apache.mahout.clustering Cluster]
           [org.apache.mahout.math Vector]
           [java.awt.geom Ellipse2D$Double]
           [org.jfree.chart.annotations XYShapeAnnotation XYTextAnnotation]
           [org.mediavirus.parvis.gui MainFrame]))

(defmulti to-point class)

(defmethod to-point clojure.lang.PersistentVector
  [v] v)

(defmethod to-point clojure.lang.PersistentList
  [l] l)

(defmethod to-point org.apache.mahout.math.Vector
  [v] (let [elems (iterator-seq (.iterator v))]
        (map (fn [elem] (.get elem)) elems)))

(defmethod to-point extramuros.java.formats.Row
  [r] (map identity (.getValues r)))

(defmethod to-point org.apache.mahout.math.VectorWritable
  [v] (to-point (.get v)))

(defmethod to-point org.apache.mahout.clustering.WeightedVectorWritable
  [v] (to-point (.getVector v)))

(defmethod to-point org.apache.mahout.clustering.Cluster
  [v] (to-point (.getCenter v)))

(defmulti to-ellipse class)

(defmethod to-ellipse clojure.lang.PersistentArrayMap
  [m] (let [x (or (:x m) (get m "x"))
            y (or (:y m) (get m "y"))
            w (or (:w m) (get m "w"))
            h (or (:h m) (get m "h"))]
        (XYShapeAnnotation. (Ellipse2D$Double. (- x w) (- y h)  (* 2 w) (* 2 h)))))

(defmethod to-ellipse clojure.lang.PersistentVector
  [v] (let [x (nth v 0)
            y (nth v 1)
            w (nth v 2)
            h (nth v 3)]
        (XYShapeAnnotation. (Ellipse2D$Double. (- x w) (- y h)  (* 2 w) (* 2 h)))))

(defmethod to-ellipse clojure.lang.PersistentList
  [l] (let [x (nth l 0)
            y (nth l 1)
            w (nth l 2)
            h (nth l 3)]
        (XYShapeAnnotation. (Ellipse2D$Double. (- x w) (- y h)  (* 2 w) (* 2 h)))))

(defn plot-label
  ([[plot data points] txt x y]
     (.addAnnotation (.getPlot plot) (XYTextAnnotation. (str txt) x y))
     [plot data points]))

(defn plot-centroid
  [[plot data points] c xdim ydim]
  (let [center (cluster-center-vector c)
        radius (cluster-radius-vector c)
        [x y] [(nth center xdim) (nth center ydim)]
        [w h] [(nth radius xdim) (nth center ydim)]]
    (.addAnnotation (.getPlot plot) (to-ellipse {:x x :y y :w w :h h}))
    (plot-label [plot data points] (cluster-id c) x y)))

(defn scatter-plot-for
  ([data & opts]
     (let [points (map to-point data)
           opts (apply hash-map opts)
           x-label (or (:x-label opts) "")
           y-label (or (:y-label opts) "")
           title (or (:title opts) "")
           legend (or (:legend opts) true)]
       [{:x-label x-label :y-label y-label :title title :legend legend} data points])))

(defn plot-data
  ([[plot data points] new-data]
     [plot new-data (map to-point new-data)]))

(defn plot-dimensions [[plot data points] x y & opts]
  (let [opts (apply hash-map opts)
        plot (if (map? plot)
               (scatter-plot (map #(nth % x) points)
                             (map #(nth % y) points)
                             :x-label (:x-label plot)
                             :y-label (:y-label plot)
                             :legend (:legend plot)
                             :title (:title plot)
                             :series-label (or (:label opts) ""))
               (add-points plot
                           (map #(nth % x) points)
                           (map #(nth % y) points)
                           :series-label (or (:label opts) "")))]
       [plot data points]))

(defn plot-parallel-bars
  ([table] (javax.swing.UIManager/put "org.mediavirus.parvis.gui.ParallelDisplayUI" "org.mediavirus.parvis.gui.BasicParallelDisplayUI")
     (.show (MainFrame. (:table table) nil)))
  ([table columns]
     (let [columns-array (make-array String (count columns))]
       (doseq [i (range 0 (count columns))]
         (aset columns-array i (nth columns i)))
       (javax.swing.UIManager/put "org.mediavirus.parvis.gui.ParallelDisplayUI" "org.mediavirus.parvis.gui.BasicParallelDisplayUI")
       (.show (MainFrame. (:table table) columns-array)))))


(defmulti view-table-dispatcher
  (fn [visualization table options] visualization))

(defmethod view-table-dispatcher :parallel-lines
  ([_ table options]
     (if (:columns options)
       (plot-parallel-bars table (:columns options))
       (plot-parallel-bars table))))

(defmethod view-table-dispatcher :box-plot
  ([_ table options]
     (if (:columns (or options {}))
       (map (fn [column]
              (let [column-position (table-column-position column table)
                    points (map (fn [row]
                                  (nth (row-to-seq row) column-position))
                                (table-rows table))]
                (view (box-plot points
                                :title (or (:title options)
                                           (str "table " (table-rows-file table) " - " column))
                                :legend (or (:legend options) true)
                                :x-label (or (:x-label options) column)
                                :y-label (or (:y-label options) "frequency")
                                :series-label (or (:series-label options) column)))))
            (:columns options))
       (view-table-dispatcher :box-plot table (assoc (or options {}) :columns (table-numeric-columns table))))))

(defmethod view-table-dispatcher :scatter-plot
  ([_ table options]
     (if (:columns options)
       (let [points-range (range 0 (count (table-points-seq-for-column table (first (:columns options)))))]
         (loop [columns (:columns options)]
           (if (empty? columns)
             :ok
             (do
               (view (scatter-plot
                      points-range
                      (table-points-seq-for-column table (first columns))
                      :legend true
                      :title (str "table " (table-rows-file table) " - " (first columns))
                      :series-label (first columns)))
               (recur (rest columns))))))
       (view-table-dispatcher :scatter-plot table (assoc options :columns (table-numeric-columns table))))))


(defmethod view-table-dispatcher :scatter-plot-grouped
  ([_ table options]
     (if (:columns options)
       (let [first-column (first (:columns options))
             rest-columns (rest (:columns options))
             points-range (range 0 (count (table-points-seq-for-column table first-column)))
             plot-env (scatter-plot
                       points-range
                       (table-points-seq-for-column table first-column)
                       :title (or (:title options) (str "table " (table-rows-file table)))
                       :legend (or (:legend options) true)
                       :x-label "columns"
                       :y-label "values"
                       :series-label first-column)]
         (loop [columns (:columns options)
                plot-env plot-env]
           (if (empty? columns)
             (view plot-env)
             (do
               (add-points plot-env
                           points-range
                           (table-points-seq-for-column table (first columns))
                           :series-label (first columns))
               (recur (rest columns)
                      plot-env)))))
       (view-table-dispatcher :scatter-plot-grouped table (assoc options :columns (table-numeric-columns table))))))

(defmethod view-table-dispatcher :frequencies-histogram
  ([_ table options]
     (if (:columns options)
       (doseq [column (:columns options)]
         (view (histogram
                (table-points-seq-for-column table column)
                :x-label (or (:x-label options) "value")
                :y-label (or (:y-label options) "frequency")
                :title (or (:title options) (str "frequency distribution for column " column)))))
       (view-table-dispatcher :frequencies table (table-numeric-columns table)))))

(defn view-table
  ([visualization table options]
     (let [table  (if (:sample options)
                    (let [sampling (make-job :probabilistic-sample-table)]
                      (set-config sampling {:table table
                                            :directory-output (str "/tmp/sampling/" (.replace (str (java.util.UUID/randomUUID)) "-" ""))
                                            :sampling-probability (:sample options)})
                      (run sampling)
                      (output sampling))
                    table)]
       (view-table-dispatcher visualization table options)))
  ([visualization table]
     (view-table visualization table {})))
