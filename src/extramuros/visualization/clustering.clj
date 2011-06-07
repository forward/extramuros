(ns extramuros.visualization.clustering
  (:use [extramuros.visualization core]
        [extramuros.visualization.3d]
        [extramuros math datasets]
        [extramuros.jobs core]))

(defn build-dimensions-for-table
  ([table]
     (build-dimensions (table-ordered-columns table)
                       (table-ordered-columns table))))

(defn build-3-dimensions-for-table
  ([table]
     (build-dimensions (table-ordered-columns table)
                       (table-ordered-columns table)
                       (table-ordered-columns table))))

(defn plot-clusters-dim-xy
  ([clusters points label-x label-y table display-centroids]
     (let [x (table-column-position label-x table)
           y (table-column-position label-y table)
           [first-label first-cluster] (first clusters)
           clusters (rest clusters)
           plot-env (-> (scatter-plot-for (get points first-label)
                                          :legend (str "Dimensions " label-x " vs " label-y)
                                          :x-label label-x
                                          :y-label label-y) 
                        (plot-dimensions x y))]
       (loop [clusters clusters
              plot-env plot-env]
         (if (empty? clusters)
           (first plot-env)
           (let [[next-label next-cluster] (first clusters)
                 clusters (rest clusters)
                 plot-env (if display-centroids
                            (-> plot-env
                                (plot-data (get points next-label))
                                (plot-dimensions x y  :label (str "cluster " next-label))
                                (plot-centroid next-cluster x y))
                            (-> plot-env
                                (plot-data (get points next-label))
                                (plot-dimensions x y  :label (str "cluster " next-label))))]
             (recur clusters plot-env)))))))

(defn plot-clustering-output
  ([job opts]
     (let [clusters (output job :clusters)
           folded-points (output job :folded-points)
           table (output job :clustered-points-table)
           dimensions (build-dimensions-for-table table)]
       (map
        (fn [p] (let [[x-label y-label] p]
                 (println (str " inside " x-label " " y-label))
                 (plot-clusters-dim-xy clusters folded-points x-label y-label table (or (:display-centroids opts) true))))
        dimensions)))
  ([job x-label y-label opts]
     (let [clusters (output job :clusters)
           folded-points (output job :folded-points)
           table (output job :clustered-points-table)
           display-centroids (or (:display-centroids opts) true)]
       (plot-clusters-dim-xy clusters folded-points x-label y-label table display-centroids))))

(defn plot-clusters-dim-xyz
  ([clusters folded-points label-x label-y label-z table]
     (let [x (table-column-position label-x table)
           y (table-column-position label-y table)
           z (table-column-position label-z table)
           [first-label first-cluster] (first clusters)
           clusters (rest clusters)
           plot-env (-> (scatter-3d-plot-for (get folded-points first-label))
                        (plot-3-dimensions x y z))]
       (loop [clusters clusters
              plot-env plot-env]
         (if (empty? clusters)
           (build-3d-plot plot-env)
           (let [[next-label next-cluster] (first clusters)
                 clusters (rest clusters)
                 plot-env (-> plot-env
                              (plot-data (get folded-points next-label))
                              (plot-3-dimensions x y z))]
             (recur clusters plot-env)))))))

(defn plot-3d-clustering-output
  ([job]
     (let [clusters (output job :clusters)
           folded-points (output job :folded-points)
           table (output job :clustered-points-table)
           dimensions (build-3-dimensions-for-table table)]
       (map
        (fn [p] (let [[x-label y-label z-label] p]
                 (plot-clusters-dim-xyz clusters folded-points x-label y-label z-label table)))
        dimensions)))
  ([job x-label y-label z-label]
     (let [clusters (output job :clusters)
           folded-points (output job :folded-points)
           table (output job :clustered-points-table)]
       (plot-clusters-dim-xyz clusters folded-points x-label y-label z-label table))))
