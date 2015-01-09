(ns chronograph.import.laskuri
  (:require [clojure.string :refer [split-lines split]])
  (:require [chronograph.data :as data])
  (:require [crossref.util.config :refer [config]])
  (:require [clj-time.format :as format])
  (:require [robert.bruce :refer [try-try-again]])
  (:require [clojure.core.async :as async :refer [>! >!! go]]))


(defn insert-event-from-local-type
  [base type-name-s3 type-name source-name overwrite f]
  (prn "Insert" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        ]
    ; Iterate over all 'parts' for this type.
    (doseq [file part-files]
      (prn file)
        (let [lines (line-seq (clojure.java.io/reader file))]
              (doseq [line lines]
                (try
                (let [[doi date cnt arg1 arg2 arg3] (f line)]
                  (data/insert-event doi type-id source-id date overwrite cnt nil nil nil))
                (catch Exception e (prn "Exception" e "line" line))))))))


(defn insert-domain-event-from-local-type
  [base type-name-s3 type-name source-name overwrite f]
  (prn "Insert domain" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        ]
    ; Iterate over all 'parts' for this type.
    (doseq [file part-files]
      (prn file)
        (let [lines (line-seq (clojure.java.io/reader file))]
              (doseq [line lines]
                (try
                (let [[host domain date cnt] (f line)]
                  (data/insert-domain-event host domain type-id source-id date overwrite cnt))
                (catch Exception e (prn "Exception" e "line" line))))))))



(defn insert-subdomain-event-from-local-type
  [base type-name-s3 type-name source-name overwrite f]
  (prn "Insert subdomain" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)]
    ; Iterate over all 'parts' for this type.
    (doseq [file part-files]
      (prn file)
        (let [lines (line-seq (clojure.java.io/reader file))]
              (doseq [line lines]
                (try
                (let [[host domain date cnt] (f line)]
                  (data/insert-subdomain-event host domain type-id source-id date overwrite cnt))
                (catch Exception e (prn "Exception" e "line" line))))))))


; Line parsers for various Laskuri input formats.

(defn parse-string-timeline 
  "Take a line of «string» («date» «count»)* and return [string, {date count}]
  «period»-doi-periods-count
  «period»-domain-periods-count"
  [line]
  (let [splitten (.split #"\t" line)
        item (first splitten)
        pairs (partition 2 (rest splitten))
        timeline (map (fn [[dt cnt]] [(format/parse dt) (. Integer parseInt cnt)]) pairs)]
    [item (into {} timeline)]))

(defn parse-string-string-timeline 
  "Take a line of «string» «string» («date» «count»)* and return [string, {date count}]
  «period»-subdomain-periods-count
  "
  [line]
  (let [splitten (.split #"\t" line)
        item (first splitten)
        item2 (second splitten)
        pairs (partition 2 (drop 2 splitten))
        timeline (map (fn [[dt cnt]] [(format/parse dt) (. Integer parseInt cnt)]) pairs)]
    [item item2 (into {} timeline)]))

(defn parse-top-domains
  "Take a line of «date» (domain count)* and return [date, {domain: count}]
  «period»-top-domains"
  [line]
  (let [splitten (.split #"\t" line)
        date (format/parse (first splitten))
        pairs (partition 2 (rest splitten))
        values (map (fn [[domain cnt]] [domain (. Integer parseInt cnt)]) pairs)]
    [date (into {} values)]))

(defn parse-string-count
  "Take a line of «string» «count» and return [string count]
  ever-doi-count
  ever-domain-count"
  [line]
  (let [[string count-str] (split line #"\t")
        cnt (. Integer parseInt count-str)]
    [string cnt]))

(defn parse-string-string-count
  "Take a line of «string» «string» «count» and return [string string count]
  ever-subdomain-count"
  [line]
  (let [[string string2 count-str] (split line #"\t")
        cnt (. Integer parseInt count-str)]
    [string string2 cnt]))


(defn parse-string-date
  "Take a line of «string» «date» and return [string date]
  ever-doi-first-date"
  [line]
  (let [[string date-str] (split line #"\t")
        date (format/parse date-str)]
    [string date]))

; NEW!

(def transaction-chunk-size 100000)

(defn lazy-line-seq
  "Lazy seq of parsed lines"
  [base type-name-s3 f]
  (let [dirname (str base "/" type-name-s3 "/")
        directory (clojure.java.io/file dirname)  
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        
        ; Lazy seq of the files.
        seqs (map #(line-seq (clojure.java.io/reader %)) part-files)
        
        ; Lazy seq of all lines.
        whole (apply concat seqs)
        
        ; Lazy seq of lines parsed.
        parsed (remove nil? (map f whole))]
    parsed))

(defn insert-doi-timelines
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-timeline)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [doi timeline]
    (doseq [chunk chunks]
      (prn "insert doi timeline chunk")
      (data/insert-event-timelines chunk type-id source-id))))

(defn insert-domain-timelines
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-timeline)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [domain-host timeline]
    (doseq [chunk chunks]
      (prn "insert domain timeline chunk")
      (data/insert-domain-timelines chunk type-id source-id))))

(defn insert-subdomain-timelines
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-string-timeline)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [subdomain-host domain timeline]
    (doseq [chunk chunks]
      (prn "insert subdomain timeline chunk")
      (data/insert-subdomain-timelines chunk type-id source-id))))

(defn insert-ever-doi-count
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-count)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [doi count]
    (doseq [chunk chunks]
      (prn "insert ever-doi-count chunk")
      (data/insert-doi-resolutions-count chunk type-id source-id))))

(defn insert-ever-first-date
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-date)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [doi date]
    (doseq [chunk chunks]
      (prn "insert insert-ever-first-date chunk")
      (data/insert-doi-first-resolution chunk type-id source-id))))

(defn insert-ever-domain-count
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-count)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [domain count]
    (doseq [chunk chunks]
      (prn "insert insert-ever-domain-count chunk")
      (data/insert-domain-count chunk type-id source-id))))

(defn insert-ever-subdomain-count
  [base type-name-s3 type-name source-name]
  (let [lines (lazy-line-seq base type-name-s3 parse-string-string-count)
        chunks (partition-all transaction-chunk-size lines)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; chunks is sequence of [host domain count]
    (doseq [chunk chunks]
      (prn "insert insert-ever-subdomain-count chunk")
      (data/insert-subdomain-count chunk type-id source-id))))

; no source name because this goes into a special table and can only come from one place (logs).
(defn insert-month-top-domains
  [base type-name-s3]
  (let [lines (lazy-line-seq base type-name-s3 parse-top-domains)
        chunks (partition-all transaction-chunk-size lines)]
    ; chunks is sequence of [date {domain: count}]
    (doseq [chunk chunks]
      (prn "insert insert-month-top-domains chunk")
      (data/insert-month-top-domains chunk))))

; OLD AGAIN


(defn insert-grouped-event-from-local-type
  [base type-name-s3 type-name source-name f]
  ; (go
  (prn "Insert" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        
        ; Lazy seq of the files.
        seqs (map #(line-seq (clojure.java.io/reader %)) part-files)
        
        ; Lazy seq of all lines.
        whole (apply concat seqs)
        
        ; Lazy seq of lines parsed.
        parsed (remove nil? (map f whole))
        
        ; Grouped by DOI
        partitions (partition-by first parsed)]
    
    ; Iterate over all 'parts' for this type.
    
    (doseq [partit partitions]
    
      (let [doi (first (first partit))
            timeline (apply merge (map (fn [[doi date cnt _ _ _]] {date cnt}) partit))]
            (>!! data/event-timeline-chan [doi type-id source-id timeline (fn [old nw] nw)])
          ))))



(defn insert-grouped-domain-event-from-local-type
  [base type-name-s3 type-name source-name f]
  (prn "Insert domain" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        
        ; Lazy seq of the files.
        seqs (map #(line-seq (clojure.java.io/reader %)) part-files)
        
        ; Lazy seq of all lines.
        whole (apply concat seqs)
        
        ; Lazy seq of lines parsed.
        parsed (remove nil? (map f whole))
        
        ; Grouped by Domain
        partitions (partition-by second parsed)]
    
    ; Iterate over all 'parts' for this type.
    (doseq [partit partitions]
      (let [[host domain _ _] (first partit)
            timeline (apply merge (map (fn [[_ _ date cnt]] {date cnt}) partit))]
        (try
          (data/insert-domain-timeline host domain type-id source-id timeline (fn [old nw] nw))
          (catch Exception _ nil))))))
        
        
(defn insert-grouped-subdomain-event-from-local-type
  [base type-name-s3 type-name source-name f]
  (prn "Insert subdomain" type-name "from" base)
  (let [directory (clojure.java.io/file (str base "/" type-name-s3 "/"))  
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)
        
        files (file-seq directory)
        ; filter out self, directory, crc files etc
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        
        ; Lazy seq of the files.
        seqs (map #(line-seq (clojure.java.io/reader %)) part-files)
        
        ; Lazy seq of all lines.
        whole (apply concat seqs)
        
        ; Lazy seq of lines parsed.
        parsed (remove nil? (map f whole))
        
        ; Grouped by Domain
        partitions (partition-by second parsed)]
    
    ; Iterate over all 'parts' for this type.
    (doseq [partit partitions]
      (let [[host domain _ _] (first partit)
            timeline (apply merge (map (fn [[_ _ date cnt]] {date cnt}) partit))]
        (try
        (data/insert-subdomain-timeline host domain type-id source-id timeline (fn [old nw] nw))
        (catch Exception _ nil))))))

(defn run-local-grouped
    "Import latest Laskuri output, grouped by DOI, from a local directory. Base is the directory within the bucket, usually a timestamp."
    [base]    
    (prn "Run Laskuri Local Grouped")
    ; day-doi-periods-count
    (insert-doi-timelines base "day-doi-periods-count" "daily-resolutions" "CrossRefLogs")
    
    ; day-domain-periods-count
    (insert-domain-timelines base "day-domain-periods-count" "daily-referral-domain" "CrossRefLogs")
    
    ; day-subdomain-periods-count
    (insert-subdomain-timelines base "day-subdomain-periods-count" "daily-referral-domain" "CrossRefLogs")

    ; day-top-domains - IGNORE
    
    ; ever-doi-count
    (insert-ever-doi-count base "ever-doi-count" "total-resolutions" "CrossRefLogs")
    
    ; ever-doi-first-date
    (insert-ever-first-date base "ever-doi-first-date" "first-resolution" "CrossRefLogs")
    
    ; ever-domain-count
    (insert-ever-domain-count base "ever-domain-count" "total-referrals-domain" "CrossRefLogs")
    
    ; ever-subdomain-count
    (insert-ever-subdomain-count base "ever-subdomain-count" "total-referrals-subdomain" "CrossRefLogs")
    
    ; month-doi-periods-count - IGNORE
    ; month-domain-periods-count - IGNORE
    ; month-subdomain-periods-count - IGNORE
    
    ; month-top-domains
    (insert-month-top-domains base "month-top-domains"))