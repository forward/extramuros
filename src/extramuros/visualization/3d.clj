(ns extramuros.visualization.3d
  (:use [extramuros.jobs core]
        [extramuros.visualization core]
        [extramuros math]
        [extramuros.hdfs :only [*conf* path]]
        [extramuros.datasets :only [table-obj-to-schema]])
  (:import [java.awt Rectangle]
           [org.jzy3d.ui ChartLauncher]
           [org.jzy3d.plot3d.primitives Scatter]
           [org.jzy3d.colors Color]
           [org.jzy3d.maths Coord3d]
           [org.jzy3d.chart Chart]
           [org.jzy3d.chart.controllers.mouse ChartMouseController]
           [org.jzy3d.chart.controllers.keyboard ChartKeyController]))

(def *colors* [(Color. (java.awt.Color/BLUE)) (Color. (java.awt.Color/GREEN)) (Color. (java.awt.Color/RED)) (Color. (java.awt.Color/BLACK)) (Color. (java.awt.Color/CYAN))
               (Color. (java.awt.Color/YELLOW)) (Color. (java.awt.Color/MAGENTA))])
;(def *colors* [(Color. (java.awt.Color/RED)) (Color. (java.awt.Color/GREEN)) (Color. (java.awt.Color/BLUE))
;               (Color. (java.awt.Color/MAGENTA)) (Color. (java.awt.Color/ORANGE))
;               (Color. (java.awt.Color/PINK)) (Color. (java.awt.Color/GRAY)) (Color. (java.awt.Color/BLACK))])

(defn random-scatter
  ([num-points width heigh title]
     (let [chart (Chart.)
           points (make-array Coord3d num-points)
           colors (make-array Color num-points)]
       (doseq [i (range 0 num-points)]
         (do
           (aset points i (Coord3d. (rand 100) (rand 100) (rand 100)))
           (aset colors i (Color. (int (rand 255)) (int (rand 255)) (int (rand 255)) 5))))
       (let [scatter (Scatter. points colors)]
         (.add (.getScene chart) scatter)
         (ChartLauncher/openChart chart (Rectangle. width heigh) title)))))

(defn compute-3d-scatter-plot
  ([clustering-output dims]
     (let [chart (Chart.)
           colors (take (count (:clusters clustering-output)) (cycle *colors*))
           ;_ (println (str "COLORS: " (vec colors)))
           cluster-colors (first (reduce (fn [[h cs] cluster] [(assoc h (:id cluster) (first cs))
                                                              (rest cs)])
                                         [{} colors]
                                         (:clusters clustering-output)))
           ;_ (println (str "CLUSTER COLORS: " (vec cluster-colors)))
           points-colors (map (fn [point]
                                (if (= (.indexOf (:cluster point) "OUTLIER") 0)
                                  nil
                                  (do ;(println (str " -> " (get cluster-colors (:cluster point)) " FROM " cluster-colors " FOR " (:cluster point)))
                                      [(Coord3d. (float (nth (:components point) (nth dims 0)))
                                                 (float (nth (:components point) (nth dims 1)))
                                                 (float (nth (:components point) (nth dims 2))))
                                       (.clone (get cluster-colors (:cluster point)))])))
                              (:points clustering-output))
           points-colors (filter (comp not nil?) points-colors)
           ;_ (println (str "POINTS COLORS (" (count points-colors) ") \r\n" points-colors))
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
  ([plot]
     (view-3d plot "3d scatter plot"))
  ([plot title]
     (let [chart (Chart.)]
       (do (.add (.getScene chart) plot)
           (.addController chart (ChartMouseController.))
           (.addController chart (ChartKeyController.))
           (ChartLauncher/openChart chart (Rectangle. 500 400) title)))))

;; (def *results* (proclus-algorithm-output "hdfs://localhost:9000/user/antonio/pro_clus/result/clusters.txt","hdfs://localhost:9000/user/antonio/pro_clus/clustered_points/part-r-00000"))
;; (medoid-dimensions *results*)
;; (visualize-3d-plot (compute-3d-scatter-plot *results* [2 3 4]))
