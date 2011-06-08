(ns ^{:doc "Public API for the library"
      :author "antonio"}
  extramuros.core)

(defn def-schema
  ([& args] (apply extramuros.datasets/def-schema args)))
