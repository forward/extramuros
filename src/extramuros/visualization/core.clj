(ns ^{:doc "Graphical plotting primitives"
      :author "Antonio Garrote"}
  extramuros.visualization.core
  (:use [incanter core charts])
  (:use [extramuros.jobs core]
        [extramuros math]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema]])
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable] 
           [org.apache.mahout.clustering Cluster]
           [org.apache.mahout.math Vector]
           [java.awt.geom Ellipse2D$Double]
           [org.jfree.chart.annotations XYShapeAnnotation XYTextAnnotation]))

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
