(ns chronograph.main
  (:require [chronograph.data :as data]
            [chronograph.db :as db]
            [chronograph.import.mdapi :as mdapi]
            [chronograph.handlers :as handlers]
            [chronograph.import.laskuri :as laskuri]
            [chronograph.import.resolver :as resolver]
            [chronograph.import.first-deposit :as first-deposit])
  (:require [clojure.java.io :refer [reader]])
  (:require [clojure.edn :as edn])
  (:require [org.httpkit.server :as server])
  (:require [clj-time.format :as format])
  (:require [crossref.util.config :refer [config]]))

(defn -main
  [& args]
  
  (data/init!)
      
  (when (= (first args) "import-first-deposit")
      (prn "DOI first deposit dates")
      (first-deposit/run (second args)))
  
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
      (resolver/run-doi-resolution))
  
  (when (= (first args) "import-laskuri")
    (prn "Import local Laskuri data grouped by DOI")
    (laskuri/run-local-grouped (second args)))
  
  (when (= (first args) "server")
    (prn "Server")
    (server/run-server handlers/app {:port (:port config)})))  


; For use in REPL.
(defonce s (atom nil))

(defn stop-server
  []
  (@s)
  (reset! s nil)
  (prn "Stop Server" @s))

(defn start-server []
  (data/init!)
  (reset! s (server/run-server #'handlers/app {:port (:port config)}))
  (prn "Start Server" @s))

(defn restart-server []
  (stop-server)
  (start-server))
