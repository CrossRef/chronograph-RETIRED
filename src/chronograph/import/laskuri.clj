(ns chronograph.import.laskuri
  (:require [clojure.string :refer [split-lines split]]
            [clojure.edn :as edn]
            )
  (:require [chronograph.data :as data]
            [chronograph.db :as db])
  (:require [crossref.util.config :refer [config]])
  (:require [clj-time.core :as time])
  (:require [robert.bruce :refer [try-try-again]])
  (:require [clojure.core.async :as async :refer [>! >!! go]]))

; Line parsers for various Laskuri input formats.


(defn parse-timeline 
  "Take a line of «string» («date» «count»)* and return [string, {date count}]
  «period»-doi-periods-count
  «period»-domain-periods-count"
  [line]
  (let [splitten (edn/read-string line)
        [doi timeline] splitten
        timeline (map (fn [[dt cnt]] [(apply time/date-time dt) cnt]) timeline)]
    [doi (into {} timeline)]))


(defn parse-string-string-timeline 
  "Take a line of «string» «string» («date» «count»)* and return [string, {date count}]
  «period»-subdomain-periods-count
  "
  [line]
  (let [splitten (edn/read-string line)
        item (first splitten)
        item2 (second splitten)
        pairs (partition 2 (drop 2 splitten))
        timeline (map (fn [[dt cnt]] [(apply time/date-time dt) cnt]) pairs)]
    [item item2 (into {} timeline)]))

(defn parse-plain
  "Parse an EDN line"
  [line]
  (edn/read-string line))

(defn parse-top-domains
  "Take a line of «date» (domain count)* and return [date, {domain: count}]
  «period»-top-domains"
  [line]
  (let [splitten (edn/read-string line)
        date (apply time/date-time (first splitten))
        pairs (partition 2 (rest splitten))
        values (map (fn [[domain cnt]] [domain cnt]) pairs)]
    [date (into {} values)]))

(defn parse-string-count
  "Take a line of «string» «count» and return [string count]
  ever-doi-count
  ever-domain-count"
  [line]
  (let [[string cnt] (edn/read-string line)]
    [string cnt]))


(defn parse-string-date
  "Take a line of «string» «date» and return [string date]
  ever-doi-first-date"
  [line]
  (let [[string date-str] (edn/read-string line)
        date (apply time/date-time date-str)]
    [string date]))


(defn swallow-parse
  "Try to parse line, return nil on error"
  [f line]
  (try 
    (f line)
    (catch Exception e (prn "bad line" line e))))

(def transaction-chunk-size 10000)

(defn lazy-line-seq
  "Lazy seq of parsed lines"
  [base laskuri-name f]
  (let [dirname (str base "/" laskuri-name "/")
        directory (clojure.java.io/file dirname)  
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        
        cnt (atom 0)
        total (count part-files)
        
        ; Lazy seq of the files.
        seqs (map (fn [f]
                    (swap! cnt inc)
                    (prn (str "Part file " @cnt " / " total))
                    (line-seq (clojure.java.io/reader f))) part-files)
        
        ; Lazy seq of all lines.
        whole (apply concat seqs)
        
        ; Lazy seq of lines parsed.
        parsed (remove nil? (map #(swallow-parse f %) whole))]
    parsed))

(defn insert-doi-timelines
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-timeline)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [doi timeline]
    (doseq [chunk chunks]
      (prn "insert doi timeline chunk")
       (let [chunk (map (fn [[doi timeline]]
                         [doi (data/filter-timeline timeline)]) chunk)]
      (data/insert-doi-timelines chunk type-name source-name)))))

(defn insert-domain-timelines
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-timeline)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [domain-host timeline]
    (doseq [chunk chunks]
      (prn "insert domain timeline chunk")
      (let [chunk (map (fn [[domain timeline]]
                         [domain (data/filter-timeline timeline)]) chunk)]
      (data/insert-domain-timelines chunk type-name source-name)))))

(defn insert-subdomain-timelines
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-timeline)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [subdomain-host domain timeline]
    (doseq [chunk chunks]
      (prn "insert subdomain timeline chunk")
      (let [chunk (map (fn [[[host domain] timeline]]
                         [[host domain] (data/filter-timeline timeline)]) chunk)]
      (data/insert-subdomain-timelines chunk type-name source-name)))))

(defn insert-ever-doi-count
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-string-count)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [doi count]
    (doseq [chunk chunks]
      (prn "insert ever-doi-count chunk" type-name source-name)
      (data/insert-doi-resolutions-count chunk type-name source-name))))

(defn insert-ever-first-date
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-string-date)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [doi date]
    (doseq [chunk chunks]
      (prn "insert insert-ever-first-date chunk")
      (data/insert-doi-first-resolution chunk type-name source-name))))

(defn insert-ever-domain-count
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-string-count)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [domain count]
    (doseq [chunk chunks]
      (prn "insert insert-ever-domain-count chunk")
      (data/insert-domain-count chunk type-name source-name))))

(defn insert-ever-subdomain-count
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-plain)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [host domain count]
    (doseq [chunk chunks]
      (prn "insert insert-ever-subdomain-count chunk")
      (data/insert-subdomain-count chunk type-name source-name))))

; no source name because this goes into a special table and can only come from one place (logs).
(defn insert-month-top-domains
  [base laskuri-name]
  (let [lines (lazy-line-seq base laskuri-name parse-top-domains)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [date {domain: count}]
    (doseq [chunk chunks]
      (prn "insert insert-month-top-domains chunk")
      (data/insert-month-top-domains chunk))))

(defn insert-month-doi-domain-period-count
  [base laskuri-name type-name source-name]
  (let [lines (lazy-line-seq base laskuri-name parse-timeline)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [doi host timeline]
    (doseq [chunk chunks]
      (prn "insert doi domain timeline chunk")
      (let [chunk (map (fn [[[doi host] timeline]]
                         [[doi host] (data/filter-timeline timeline)]) chunk)]
      (data/insert-doi-domain-timelines chunk type-name source-name)))))
      
      

(defn run-local-grouped
    "Import latest Laskuri output, grouped by DOI, from a local directory. Base is the directory within the bucket, usually a timestamp."
    [base]    
    (prn "Run Laskuri Local Grouped")
    
    ; day-doi-periods-count
    (insert-doi-timelines base "day-doi-periods-count" :daily-resolutions :CrossRefLogs)
    
    ; day-domain-periods-count
    (insert-domain-timelines base "day-domain-periods-count" :daily-referral-domain :CrossRefLogs)
    
    ; day-subdomain-periods-count
    (insert-subdomain-timelines base "day-subdomain-periods-count" :daily-referral-domain :CrossRefLogs)

    ; ever-doi-count
    (insert-ever-doi-count base "ever-doi-count" :total-resolutions :CrossRefLogs)
    
    ; ever-doi-first-date
    (insert-ever-first-date base "ever-doi-first-date" :first-resolution :CrossRefLogs)
    
    ; ever-domain-count
    (insert-ever-domain-count base "ever-domain-count" :total-referrals-domain :CrossRefLogs)
    
    ; ever-subdomain-count
    (insert-ever-subdomain-count base "ever-subdomain-count" :total-referrals-subdomain :CrossRefLogs)
  
    ; month-top-domains
    (insert-month-top-domains base "month-top-domains")
    
    (insert-month-doi-domain-period-count base "month-doi-domain-periods-count" :total-referrals-domain :CrossRefLogs))