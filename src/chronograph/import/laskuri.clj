(ns chronograph.import.laskuri
  (:require [clojure.string :refer [split-lines split]])
  (:require [chronograph.data :as data])
  (:require [aws.sdk.s3 :as s3])
  (:require [crossref.util.config :refer [config]])
  (:require [clj-time.format :as format])
  (:require [robert.bruce :refer [try-try-again]]))

(def cred {:access-key (:aws-key config) :secret-key (:aws-secret config) :endpoint "s3-us-west-2.amazonaws.com"})

(defn do-all
  "Execute f for each file in the s3 path"
  [bucket base f]
  (let [resp (s3/list-objects cred bucket {:prefix base})
        objs (:objects resp)]
    (doseq [obj objs]
      (f (:key obj)))))

(defn key-prefix [base type-name]
  (str base "/" type-name "/part-"))

(defn insert-event-from-s3-type
  [bucket base type-name-s3 type-name source-name overwrite f]
  (prn "Insert" type-name "from" base)
  (let [prefix (key-prefix base type-name-s3)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; Iterate over all 'parts' for this type.
    (do-all bucket prefix
      (fn [s3-key]
        (let [content (slurp (:content (try-try-again #(s3/get-object cred bucket s3-key))))
              lines (split-lines content)]
              (doseq [line lines]
                (let [[doi date cnt arg1 arg2 arg3] (f line)]
        (data/insert-event doi type-id source-id date overwrite cnt nil nil nil))))))))

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
        part-files (filter #(re-matches #"^part-\d*$" (.getName %)) files)
        ]
    ; Iterate over all 'parts' for this type.
    (doseq [file part-files]
      (prn file)
        (let [lines (line-seq (clojure.java.io/reader file))]
              (doseq [line lines]
                (try
                (let [[host domain date cnt] (f line)]
                  (data/insert-subdomain-event host domain type-id source-id date overwrite cnt))
                (catch Exception e (prn "Exception" e "line" line))))))))


; Line parse functions
; Should return [doi, date, cnt, arg1, arg2, arg3]
(defn parse-doi-date [line]
  (let [[doi date-str] (split line #"\t")
        date (format/parse date-str)]
    [doi date nil nil nil nil]))


(defn parse-doi-date-count [line]
  (try ; TODO once the output of Laskuri is better cleaned up this should be removed.
  (let [[doi date-str count-str] (split line #"\t")
        date (format/parse date-str)
        cnt (. Integer parseInt count-str)]
    [doi date cnt nil nil nil])
  (catch Exception _ nil)))

(defn parse-host-domain-date-count [line]
  (let [[host domain date-str count-str] (split line #"\t")
        date (format/parse date-str)
        cnt (. Integer parseInt count-str)]
    [host domain date cnt]))

(defn parse-doi-count [line]
  (let [[doi count-str] (split line #"\t")
        cnt (. Integer parseInt count-str)]
    [doi nil cnt nil nil nil]))

(defn run-s3
    "Import latest Laskuri output from an S3 bucket. Base is the directory within the bucket, usually a timestamp."
    [bucket base]
    (data/delete-events-for-type "first-resolution")
    (data/delete-events-for-type "total-resolutions")
    (data/delete-events-for-type "yearly-resolutions")
    (data/delete-events-for-type "monthly-resolutions")
    (data/delete-events-for-type "daily-resolutions")
    
    (insert-event-from-s3-type bucket base "ever-doi-first-date" "first-resolution" "CrossRefLogs" false parse-doi-date)
    (insert-event-from-s3-type bucket base "ever-doi-count" "total-resolutions" "CrossRefLogs" false parse-doi-count)
    (insert-event-from-s3-type bucket base "year-doi-period-count" "yearly-resolutions" "CrossRefLogs" false parse-doi-date-count)
    (insert-event-from-s3-type bucket base "month-doi-period-count" "monthly-resolutions" "CrossRefLogs" false parse-doi-date-count)
    (insert-event-from-s3-type bucket base "day-doi-period-count" "daily-resolutions" "CrossRefLogs" false parse-doi-date-count))


(defn run-local
    "Import latest Laskuri output from a local directory. Base is the directory within the bucket, usually a timestamp."
    [base]
    ; (data/delete-events-for-type "first-resolution")
    ; (data/delete-events-for-type "total-resolutions")
    ; (data/delete-events-for-type "yearly-resolutions")
    ; (data/delete-events-for-type "monthly-resolutions")
    ; (data/delete-events-for-type "daily-resolutions")
    ; (data/delete-domain-events-for-type "daily-referral-domain")
    ; (data/delete-domain-events-for-type "weekly-referral-domain")
    ; (data/delete-domain-events-for-type "monthly-referral-domain")
    ; (data/delete-subdomain-events-for-type "daily-referral-subdomain")
    ; (data/delete-subdomain-events-for-type "monthly-referral-subdomain")
    ; (data/delete-subdomain-events-for-type "yearly-referral-subdomain")
    
    ; (insert-event-from-local-type base "ever-doi-first-date" "first-resolution" "CrossRefLogs" false parse-doi-date)
    ; (insert-event-from-local-type base "ever-doi-count" "total-resolutions" "CrossRefLogs" false parse-doi-count)
    (insert-event-from-local-type base "year-doi-period-count" "yearly-resolutions" "CrossRefLogs" false parse-doi-date-count)
    ; (insert-event-from-local-type base "month-doi-period-count" "monthly-resolutions" "CrossRefLogs" false parse-doi-date-count)
    ; (insert-event-from-local-type base "day-doi-period-count" "daily-resolutions" "CrossRefLogs" false parse-doi-date-count)
    
    ; (insert-domain-event-from-local-type base "day-domain-period-count" "daily-referral-domain" "CrossRefLogs" false parse-host-domain-date-count)
    ; (insert-domain-event-from-local-type base "month-domain-period-count" "monthly-referral-domain" "CrossRefLogs" false parse-host-domain-date-count)
    (insert-domain-event-from-local-type base "year-domain-period-count" "yearly-referral-domain" "CrossRefLogs" false parse-host-domain-date-count)
    
    ; (insert-subdomain-event-from-local-type base "day-subdomain-period-count" "daily-referral-subdomain" "CrossRefLogs" false parse-subdomain-date-count)
    ; (insert-subdomain-event-from-local-type base "month-subdomain-period-count" "monthly-referral-subdomain" "CrossRefLogs" false parse-host-domain-date-count)
    (insert-subdomain-event-from-local-type base "year-subdomain-period-count" "yearly-referral-subdomain" "CrossRefLogs" false parse-host-domain-date-count)
    )

; TODO code duplication

(defn insert-grouped-event-from-local-type
  [base type-name-s3 type-name source-name f]
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
        (try
          ; SQL exception will be logged. 
          (data/insert-event-timeline doi type-id source-id timeline (fn [old nw] nw))
          (catch Exception _))))))


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
    (insert-grouped-event-from-local-type base "day-doi-period-count" "daily-resolutions" "CrossRefLogs" parse-doi-date-count)
    (insert-grouped-domain-event-from-local-type base "day-domain-period-count" "daily-referral-domain" "CrossRefLogs" parse-host-domain-date-count)
    (insert-grouped-subdomain-event-from-local-type base "day-subdomain-period-count" "daily-referral-subdomain" "CrossRefLogs" parse-host-domain-date-count))