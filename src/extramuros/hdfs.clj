(ns ^{:doc "Functions for raw interaction with the HDFS cluster"
      :author "Antonio Garrote"}
  extramuros.hdfs
  (:import [org.apache.hadoop.conf Configuration] 
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io Text Writable LongWritable]))

;; Configuration

(def *fs* nil)
(def *conf* nil)

(defn path
  ([path-str] (if (string? path-str) (Path. path-str) path-str)))

(defn path-to-string
  ([path]
     (if (string? path) path
         (.toString (.getPath (.toUri path))))))

(defn suffix
  [path-str suffix-str]
  (.suffix (path path-str) suffix-str))

(defn exists?
  ([path-str]
     (.exists *fs* (path path-str))))

(defn mkdir
  ([path-str]
     (.mkdirs *fs* (path path-str))))

(defn configuration
  ([] (Configuration.))
  ([path-str & others]
     (let [conf (Configuration.)]
       (.addResource conf (path path-str))
       (loop [pstr others]
         (if (empty? others)
           conf
           (do (.addResource conf (path (first pstr)))
               (recur (rest pstr))))))))

(defn file-system
  ([] (FileSystem/get *conf*)))

(defn set-file-system!
  ([]
     (let [fs (file-system)]
       (alter-var-root #'*fs* (constantly fs)))))

(defn set-config!
  ([] (let [conf (configuration)]
        (alter-var-root #'*conf* (constantly conf))))
  ([& paths] (let [conf (apply configuration paths)]
               (alter-var-root #'*conf* (constantly conf)))))

(defn bootstrap!
  ([] (do (println "default bootstrap")
          (set-config!)
          (set-file-system!)
          :ok))
  ([& config-files]
     (if (or (empty? config-files) (nil? (first config-files)))
       (bootstrap!)
       (do (apply set-config! config-files)
           (set-file-system!)
           :ok))))

(defn ls
  ([path-str] (vec (.listStatus *fs* (path path-str)))))

(defn delete
  ([f] (if (or (instance? org.apache.hadoop.fs.Path f) (string? f))
         (.delete *fs* (path f))
         (.delete *fs* (.getPath f)))))

(defn paths
  ([files] (map (fn [f] (.getPath f)) files)))

(defn show
  ([objects]
     (doseq [object objects]
       (println (str " - " object)))
     objects))

;; HDFS IO

(defn seq-file-reader
  ([path-str]
     (org.apache.hadoop.io.SequenceFile$Reader. *fs* (path path-str) *conf*)))

(defn seq-file-writer
  ([path-str key-class val-class]
     (let [iow  (org.apache.hadoop.io.SequenceFile$Writer. *fs* *conf* (path path-str) key-class val-class)]
       iow)))

(defn seq-file-long-vector
  ([path-str]
     (seq-file-writer path-str org.apache.hadoop.io.LongWritable org.apache.mahout.math.VectorWritable)))

(defn seq-file-write!
  ([writer wrapper data-pairs]
     (loop [data data-pairs]
       (if (empty? data)
         (do (.close writer)
             :ok)
         (let [[^org.apache.hadoop.io.Writable kw
                ^org.apache.hadoop.io.Writable vw]
               (apply wrapper (first data))]
           (do
             (.append writer kw vw)
             (recur (rest data))))))))


(defmacro wrapper
  ([class-key class-value]
     `(fn [k# v#]
        (let [kw# (new ~class-key)
              vw# (new ~class-value)]
          (.set kw# k#)
          (.set vw# v#)
          [kw# vw#]))))

(defn wrapper-identity
  ([] (fn [k w] [k w])))

(defmacro wrapper-identity-value
  ([class-key]
     `(fn [k# v#]
        (let [kw# (new ~class-key)]
          (.set kw# k#)
          [kw# v#]))))

(defn pairs
  ([seq-file-reader]
     (let [key (.newInstance (.asSubclass (.getKeyClass seq-file-reader) Writable))
           val (.newInstance (.asSubclass (.getValueClass seq-file-reader) Writable))]
       (let [exists (.next seq-file-reader key val)]
         (if exists
           (lazy-seq (cons [key val] (pairs seq-file-reader)))
           nil)))))

(defn vector-to-seq
  ([v] (let [elems (iterator-seq (.iterator v))]
         (map (fn [elem] (.get elem)) elems))))

(defn key-to-int
  ([k] (try
         (.get k)
         (catch Exception ex
           (.toString k)))))

(defn pair-to-clustered-vector
  ([[k v]] {:cluster (key-to-int k)
            :components (vector-to-seq (try (.getVector v)
                                            (catch Exception ex
                                              (.get v))))}))
