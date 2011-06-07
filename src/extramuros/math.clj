(ns {:doc "Math utilities and mathematical types manipulation"
     :author "antonio"}
  extramuros.math
  (:import [org.apache.mahout.clustering Cluster]
           [org.apache.mahout.math Vector]
           [org.apache.mahout.common.distance EuclideanDistanceMeasure]))

;; vectors
(defn vector-to-seq
  ([v] (let [elems (iterator-seq (.iterator v))]
         (map (fn [elem] (.get elem)) elems))))

;; clusters
(defn cluster-id
  ([c] (.getId c)))

(defn cluster-center-vector
  ([c] (vector-to-seq (.getCenter c))))

(defn cluster-radius-vector
  ([c] (vector-to-seq (.getRadius c))))


(defn pair-to-cluster
  ([[k c]] {:id (cluster-id c)
            :center (cluster-center-vector c)
            :radius (cluster-radius-vector c)}))

(defn cluster-center
  ([c] (.getCenter c)))

(defn cluster-radius
  ([c] (.getRadius c)))

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
