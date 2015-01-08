(ns chronograph.main
  (:require [chronograph.data :as d]
            [chronograph.mdapi :as mdapi])
  (:require [chronograph.import.laskuri :as laskuri])
  (:require [clojure.java.io :refer [reader]])
  (:require [clojure.edn :as edn])
  (:require [clj-time.format :as format]))

(defn -main
  [& args]
  
  (when (= (first args) "import-laskuri-s3")
    (prn "Laskuri import from" (second args) (nth args 2))
    (laskuri/run-s3 (second args) (nth args 2))) 
  
  (when (= (first args) "import-laskuri-local")
    (prn "Laskuri import from" (second args))
    (laskuri/run-local (second args))) 
  
  (when (= (first args) "import-ever")
      (prn "DOI import everything ever")
      (mdapi/get-num-dois-updated-since-async nil))
  
  (when (= (first args) "new-updates")
      (prn "DOI import new updates")
      (mdapi/run-doi-extraction-new-updates))
  
  ; (when (= (first args) "resolve")
  ;     (prn "DOI Resolution")
  ;     (d/run-doi-resolution))
  
  
  (when (= (first args) "grouped-laskuri-local")
    (prn "Import local Laskuri data grouped by DOI")
    (laskuri/run-local-grouped (second args)))

 )  
