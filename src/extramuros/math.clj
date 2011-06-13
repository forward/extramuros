(ns ^{:doc "Math utilities and mathematical types manipulation"
      :author "antonio"}
  extramuros.math
  (:use [extramuros.hdfs :only [vector-to-seq]])
  (:import [org.apache.mahout.clustering Cluster]
           [org.apache.mahout.math Vector]
           [org.apache.mahout.common.distance EuclideanDistanceMeasure]))

;; clusters
(defn cluster-id
  ([c] (.getId c)))

(defn cluster-center-vector
  ([c] (vector-to-seq (.getCenter c))))

(defn cluster-radius-vector
  ([c] (vector-to-seq (.getRadius c))))

(defn cluster-center-vector-obj
  ([c] (.getCenter c)))

(defn cluster-distance-to-cluster
  ([c1 c2]
     (.getDistanceSquared (cluster-center-vector-obj c1) (cluster-center-vector-obj c2))))

(defn pair-to-cluster
  ([[k c]] {:id (cluster-id c)
            :center (cluster-center-vector c)
            :radius (cluster-radius-vector c)}))

(defn cluster-center
  ([c] (.getCenter c)))

(defn cluster-radius
  ([c] (.getRadius c)))

(defn cluster-num-points
  ([c] (.getNumPoints c)))

;; distance measures

(defn build-distance
  [kind]
  (condp = kind
      :euclidean (EuclideanDistanceMeasure.)
      (EuclideanDistanceMeasure.)))

;; utils
(defn build-dimensions
  ([xs ys]
     (->> (for [x xs y ys] [x y])
          (map sort)
          (map vec) set vec sort
          (filter (fn [[x y]] (not= x y)))))
  ([xs ys zs]
     (->> (for [x xs y ys z zs] [x y z])
          (map sort)
          (map vec) set vec sort
          (filter (fn [[x y z]] (and (not= x y) (not= y z) (not= x z)))))))

;; pairs

(defn pair-to-seq
  ([pair] [(.getFirst pair) (.getSecond pair)]))
