(ns chronograph.import.first-deposit
  (:require [clojure.string :refer [split-lines split]])
  (:require [chronograph.data :as data])
  (:require [crossref.util.config :refer [config]])
  (:require [clj-time.format :as format])
  (:require [clojure.data.csv :as csv]))

(def date-format (format/formatter "dd-MMMM-yy"))

(def partition-size 10000)

(defn run [input-path]
  (let [lines (csv/read-csv (clojure.java.io/reader input-path))
        ; drop header
        lines (rest lines)
        
        parsed (map (fn [[doi date-str]] [doi (format/parse date-format date-str)]) lines)    
        
        ; map to format for data/insert-events-chunk-type-source
        chunk-format (map (fn [[doi date]] [doi date 1 nil nil nil]) parsed)
        
        ; chunks
        chunks (partition-all partition-size chunk-format)]
  (prn "Insert first deposit")
  (doseq [chunk chunks]
    (prn "Chunk")
    (data/insert-events-chunk-type-source chunk :deposited :CrossRefDeposit))))