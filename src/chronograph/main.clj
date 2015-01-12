(ns chronograph.main
  (:require [chronograph.data :as d]
            [chronograph.mdapi :as mdapi])
  (:require [chronograph.import.laskuri :as laskuri])
  (:require [clojure.java.io :refer [reader]])
  (:require [clojure.edn :as edn])
  (:require [clj-time.format :as format]))

(defn -main
  [& args]
  
  (when (= (first args) "update-member-domains")
      (prn "update-member-domains")
      (mdapi/update-member-domains))
  
  (when (= (first args) "import-ever")
      (prn "DOI import everything ever")
      (mdapi/get-dois-updated-since nil))
  
  (when (= (first args) "new-updates")
      (prn "DOI import new updates")
      (mdapi/run-doi-extraction-new-updates))
  
  (when (= (first args) "resolve")
      (prn "DOI Resolution")
      (d/run-doi-resolution))
  
  (when (= (first args) "import-laskuri")
    (prn "Import local Laskuri data grouped by DOI")
    (laskuri/run-local-grouped (second args))))  
