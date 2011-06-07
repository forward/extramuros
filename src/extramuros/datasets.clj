(ns ^{:doc "Core functions to manipulate datasets and tables:
             - import, wrap, write, open datasets
             - Table and Row Java objects manipulation
             - Table interface"
      :author "antonio"}
  extramuros.datasets
  (:import (extramuros.java.formats Row Table RowTypes TableHeader)
           (org.apache.hadoop.io LongWritable)
           (java.util HashMap ArrayList))
  (:use (extramuros hdfs)
        [clojure.contrib.duck-streams :only [reader read-lines]]))

(def *string* RowTypes/STRING)
(def *float* RowTypes/FLOAT)
(def *double* RowTypes/DOUBLE)
(def *integer* RowTypes/INTEGER)
(def *long* RowTypes/LONG)
(def *categorical* RowTypes/CATEGORICAL)

(defn numeric-type?
  "Checks if a datum is numeric"
  ([type] (if (or (= type *float*)
                  (= type *double*)
                  (= type *integer*)
                  (= type *long*))
            true false)))

(defn def-schema
  "Defines a new schema from a sequence of column names and data types"
  ([& pairs]
     (let [columns-map (apply hash-map pairs)
           ordered-columns (loop [ws pairs ac [] c 0]
                             (if (empty? ws) ac
                                 (if (even? c)
                                   (recur (rest ws) (conj ac (first ws)) (inc c))
                                   (recur (rest ws) ac (inc c)))))]
       {:columns-map columns-map
        :ordered-columns ordered-columns})))

(defn ordered-types-schema
  "Returns the types for the ordered list of columns in a table"
  ([schema]
     (map (fn [k] (get (:columns-map schema) k)) (:ordered-columns schema))))

(defn parse-datum
  "Given a string containing a datum and type information tries to parse the string to the provided kind"
  ([datum kind]
     (condp = kind
         *string* (str datum)
         *float*  (if (string? datum) (Float. (Float/parseFloat datum)) (Float. (float datum)))
         *double*  (if (string? datum) (Double. (Double/parseDouble datum)) (Double. (double datum)))
         *integer*  (if (string? datum) (Integer. (Integer/parseInt datum)) (Integer. (int datum)))         
         *categorical* (str datum)
         (throw (Exception. (str "Uknown type for row " kind))))))

(defn parse-writable
  "Tries to extract the object wrapped in a Writable container object"
  ([writable]
     (try (.get writable)
          (catch Exception ex
            (if (= org.apache.hadoop.io.Text (class writable))
              (.toString writable)
              writable)))))

(defn make-row
  "Creates a new Row object with the given ID, schema and data"
  ([id schema data]
     (loop [columns (:ordered-columns schema)
            data data
            values []]
       (if (empty? columns)
         ;; return the values
         (Row. id
               (ArrayList. (map (fn [k] (name k)) (:ordered-columns schema)))
               (ArrayList. (ordered-types-schema schema))
               (ArrayList. values))
         ;; keep on building the row
         (let [column (first columns)
               value (parse-datum (first data) (get (:columns-map schema) column))]
           (recur (rest columns)
                  (rest data)
                  (conj values value)))))))

(defn make-table-header
  "Creates a new TableHeader object for the provided schema map"
  ([schema]
     (let [column-names (ArrayList. (map (fn [k] (name k)) (:ordered-columns schema)))
           column-types (ArrayList. (ordered-types-schema schema))]
       (TableHeader. column-names column-types))))

(defn write-table
  "Writes the table information to the HDFS file system.
   Different parameters can be provided:
       - table -> writes the table object to the tablePath of the table
       - table, output-path -> writes the table object to the provided path
       - output-file rows-file schema -> creates a new table object for the provided
                                         rows and schema and writes it in the output-file path "
  ([table output-path]
     (.setTablePath (:table table) output-path)
     (.save (:table table))
     (assoc table :path (.getTablePath (:table table))))
  ([table]
     (write-table table (.getTablePath (:table table))))
  ([output-file-path rows-file-path schema]
     (let [column-names (ArrayList. (map (fn [k] (name k)) (:ordered-columns schema)))
           column-types (ArrayList. (ordered-types-schema schema))
           table-header (TableHeader. column-names column-types)
           table (Table. table-header rows-file-path)]
       (seq-file-write!
        (seq-file-writer output-file-path org.apache.hadoop.io.LongWritable Table)
        (wrapper-identity-value org.apache.hadoop.io.LongWritable)
        [[0 table]])
       table)))


(defn table-obj-to-schema
  "Builds a schema map from the TableHeader information in a Table object"
  ([table]
     (let [column-names (.getColumnNames (.getHeader table))
           column-types (.getColumnTypes (.getHeader table))
           column-map   (reduce (fn [ac i] (assoc ac (nth column-names i) (nth column-types i))) {} (range 0 (count column-names)))]
       {:columns-map column-map
        :ordered-columns column-names})))

(defn open-dataset
  "Reads a table stored in HDFS, returns a table hash"
  ([table-file-path]
     (let [reader (seq-file-reader table-file-path)
           table (second (first (pairs reader)))
           _ (.setConfiguration table *conf*)]
       (.setTablePath table table-file-path)
       (.close reader)
       {:table table
        :path table-file-path
        :schema (table-obj-to-schema table)} )))

(defn import-dataset
  "creates a new table from a local file system file, returns the table hash for the new table"
  ([in-filename out-filename schema & {:keys [delim keyword-headers quote skip header compress-delim filter mapper]
                                       :or   {delim \,
                                              quote \"
                                              skip 0
                                              header false
                                              filter (constantly true)
                                              mapper identity
                                              keyword-headers true}}]
     (let [lines (read-lines (reader in-filename))
           skip (if header (inc skip) skip)]
       (loop [skip skip
              lines lines
              acum []]
         (if (empty? lines)
        
           (do (seq-file-write! (seq-file-writer (str out-filename ".rows") LongWritable Row)
                            (wrapper LongWritable Row)
                            (first (reduce (fn [[ac i] line]
                                             (print ".")
                                             [(conj ac [(long i) (make-row i schema line)]) (inc i)])
                                           [[] 0]
                                           acum)))
               (write-table out-filename (str out-filename ".rows") schema)
               (open-dataset out-filename))
           (if (> skip 0)
         
             (recur (dec skip) (rest lines) acum)
         
             (let [line (first lines)]
               (if (filter line)
             
                 (let [_ (print ".")
                       line (vec (.split line (str delim)))]
                   (recur skip (rest lines)
                          (conj acum (mapper line))))
             
                 (recur skip (rest lines) acum)))))))))

(defmulti wrap-dataset
  "Wraps a file or set of files stored in the HDFS filesystem with
   the same format using certain schema information.
   The wrapped dataset can be used as it were a native table"
  (fn [kind input output schema opts] kind))

(defmethod wrap-dataset :text [_ input output schema opts]
  (let [table (extramuros.java.formats.adapters.TextFileTableAdapter.)]
    (doto table
      (.setDefaultSeparator (:separator opts))
      (.setRowsPath input)
      (.setTablePath output)
      (.setConfiguration *conf*)
      (.setHeader (make-table-header schema)))
    (seq-file-write!
     (seq-file-writer (path (.getTablePath table))
                      org.apache.hadoop.io.LongWritable
                      extramuros.java.formats.adapters.TextFileTableAdapter)
     (wrapper-identity-value org.apache.hadoop.io.LongWritable)
     [[0 table]])
    {:table table
     :path (.getTablePath table)
     :schema (table-obj-to-schema table)}))

;; table interface


(defn row-to-seq
  "Parses a row and returns a sequence of basic types"
  ([row]
     (.getValues row)))

(defn table-rows
  "Returns a sequence of rows in the table"
  ([table]
     (iterator-seq (.iterator (:table table)))))

(defn table-schema
  "Returns the schema information for the table as a map"
  ([table]
     (:schema table)))

(defn table-ordered-columns
  "List of ordered columns"
  ([table]
     (:ordered-columns (table-schema table))))

(defn table-column-position
  "Position of a column in the lis of columns"
  ([column-name table]
     (let [[found pos] (reduce (fn [[found p] column] (if (not found)
                                                       (if (= (name column) column-name)
                                                         [true p]
                                                         [false (inc p)])
                                                       [found p]))
                               [false 0]
                               (table-ordered-columns table))]
       (if found pos nil))))

(defn table-column-type
  "Returns the column type for a column name (as an int)"
  ([column-name table]
     (get (:columns-map (table-schema table)) column-name)))

(defn table-numeric-row?
  "Checks if one row is numeric"
  ([column-name table]
     (numeric-type? (table-column-type column-name table))))

(defn table-numeric-rows
  "Returns the numeric columns for a table"
  ([table]
     (let [columns (table-ordered-columns table)]
       (filter (fn [column-name] (table-numeric-row? column-name table)) columns))))
