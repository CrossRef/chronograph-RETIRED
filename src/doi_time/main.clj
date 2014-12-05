(ns doi-time.main
  (:require [doi-time.data :as d]))

(defn -main
  [& args]
  (prn "DOI Extraction")
  (d/run-doi-extraction)
  (prn "DOI Resolution")
  (d/run-doi-resolution))
