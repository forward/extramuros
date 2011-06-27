(ns ^{:doc "3D graphical primitives"
      :author "Antonio Garrote"}
  extramuros.visualization.3d
  (:use [extramuros.jobs core]
        [extramuros.visualization core]
        [extramuros math]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema table-rows table-column-position]])
  (:import [java.awt Rectangle]
           [org.jzy3d.ui ChartLauncher]
           [org.jzy3d.plot3d.primitives Scatter]
           [org.jzy3d.colors Color]
           [org.jzy3d.maths Coord3d]
           [org.jzy3d.chart Chart]
           [org.jzy3d.chart.controllers.mouse ChartMouseController]
           [org.jzy3d.chart.controllers.keyboard ChartKeyController]))

(def *colors* [(Color. (java.awt.Color/BLUE)) (Color. (java.awt.Color/RED)) (Color. (java.awt.Color/BLACK)) (Color. (java.awt.Color/CYAN)) (Color. (java.awt.Color/YELLOW)) (Color. (java.awt.Color/MAGENTA)) (Color. (java.awt.Color/GREEN))])

(defn compute-3d-scatter-plot
  ([clustering-output dims]
     (let [chart (Chart.)
           colors (take (count (:clusters clustering-output)) (cycle *colors*))
           cluster-colors (first (reduce (fn [[h cs] cluster] [(assoc h (:id cluster) (first cs))
                                                              (rest cs)])
                                         [{} colors]
                                         (:clusters clustering-output)))
           points-colors (map (fn [point]
                                (if (= (.indexOf (:cluster point) "OUTLIER") 0)
                                  nil
                                  (do [(Coord3d. (float (nth (:components point) (nth dims 0)))
                                                 (float (nth (:components point) (nth dims 1)))
                                                 (float (nth (:components point) (nth dims 2))))
                                       (.clone (get cluster-colors (:cluster point)))])))
                              (:points clustering-output))
           points-colors (filter (comp not nil?) points-colors)
           [points-array colors-array] (reduce (fn [[pa ca i] [p c]] (do (aset pa i p)
                                                                        (aset ca i c)
                                                                        [pa ca (inc i)]))
                                               [(make-array Coord3d (count points-colors))
                                                (make-array Color   (count points-colors))
                                                0]
                                               points-colors)]
       (Scatter. points-array colors-array))))

(defn scatter-3d-plot-for
  ([data]
     (let [points (map to-point data)
           colors (take 1000 (cycle *colors*))]
       [[[] colors] data points])))

(defn plot-3-dimensions
  [[[acum colors] data points]  x y z]
  (let [colors (rest colors)
        points (map (fn [p] [(Coord3d. (float (nth p x))
                                     (float (nth p y))
                                     (float (nth p z)))
                            (.clone (first colors))])
                    points)]
    [[(concat acum points) colors] data points]))

(defn build-3d-plot
  ([[[acum colors] data points]]
     (let [[points-array colors-array] (reduce (fn [[pa ca i] [p c]] (do (aset pa i p)
                                                                        (aset ca i c)
                                                                        [pa ca (inc i)]))
                                               [(make-array Coord3d (count acum))
                                                (make-array Color   (count acum))
                                                0]
                                               acum)]
       (Scatter. points-array colors-array))))

(defn view-3d
  "Displays a 3D plot"
  ([plot]
     (view-3d plot "3d scatter plot"))
  ([plot title]
     (let [chart (org.jzy3d.chart.Chart.)]
       (do (.add (.getScene chart) plot)
           (.addController chart (org.jzy3d.chart.controllers.mouse.ChartMouseController.))
           (.addController chart (org.jzy3d.chart.controllers.keyboard.ChartKeyController.))
           (org.jzy3d.ui.ChartLauncher/openChart chart (java.awt.Rectangle. 500 400) title)))))

(defmethod view-table-dispatcher :scatter-3d
  ([_ table options]
     (if (:columns options)
       (if (not= (count (:columns options)) 3)
         (throw (Exception. "Three columns must be provided"))
         (let [column-x (nth (:columns options) 0)
               column-y (nth (:columns options) 1)
               column-z (nth (:columns options) 2)]
           (view-3d
            (-> (scatter-3d-plot-for (table-rows table))
                (plot-3-dimensions (table-column-position column-x table)
                                   (table-column-position column-y table)
                                   (table-column-position column-z table))
                (build-3d-plot)))))
       (throw (Exception. "Three columns from the table must be provided")))))
