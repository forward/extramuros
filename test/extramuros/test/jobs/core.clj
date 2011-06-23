(ns extramuros.test.jobs.core
  (:use [clojure.test])
  (:use [extramuros.jobs.core]))

(deftest test-clojure-eval
  (let [x 6
        fn-str (clojure `(fn [m] (> ~x m)))]
    (is (not ((eval (read-string fn-str)) 8)))
    (is ((eval (read-string fn-str)) 4))))
