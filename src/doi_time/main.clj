(ns doi-time.main
  (:require [doi-time.data :as d])
  (:require [clojure.java.io :refer [reader]])
  (:require [clojure.edn :as edn])
  (:require [clj-time.format :as format]))

(defn -main
  [& args]
  
  (when (= (first args) "import-ever")
      (prn "DOI import everything ever")
      (d/run-doi-extraction-ever))
  
  (when (= (first args) "new-updates")
      (prn "DOI import new updates")
      (d/run-doi-extraction-new-updates))
  
  (when (= (first args) "resolve")
      (prn "DOI Resolution")
      (d/run-doi-resolution))
 
  (when (= (first args)) "import-earliest-resolution-log"
    ; Import a list of files of doi mapped to earliest known resolution from the referral logs.
    (let [file-paths (rest args)]
        (prn "Import earliests from" file-paths)
        (doseq [file-path file-paths]
          (prn "Process" file-path)
          (let [rdr (reader file-path)
                lines (line-seq rdr)]
            (doseq [line lines]
              (let [[the-doi the-date] (edn/read-string line)
                    the-date (format/parse the-date)]
                (d/set-first-resolution-log the-doi the-date))))))))
  
