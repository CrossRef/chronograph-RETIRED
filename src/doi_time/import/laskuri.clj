(ns doi-time.import.laskuri
  (:require [clojure.string :refer [split-lines split]])
  (:require [doi-time.data :as data])
  (:require [aws.sdk.s3 :as s3])
  (:require [crossref.util.config :refer [config]])
  (:require [clj-time.format :as format]))

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

(defn insert-event-from-type
  [bucket base type-name-s3 type-name source-name overwrite f]
  (let [prefix (key-prefix base type-name-s3)
        type-id (data/get-type-id-by-name type-name)
        source-id (data/get-source-id-by-name source-name)]
    ; Iterate over all 'parts' for this type.
    (do-all bucket prefix
      (fn [s3-key]
        (let [content (slurp (:content (s3/get-object cred bucket s3-key)))
              lines (split-lines content)]
              (doseq [line lines]
                (let [[doi date cnt arg1 arg2 arg3] (f line)]
        (data/insert-event doi type-id source-id date overwrite cnt nil nil nil))))))))

; Line parse functions
; Should return [doi, date, cnt, arg1, arg2, arg3]
(defn parse-doi-date [line]
  (let [[doi date-str] (split line #"\t")
        date (format/parse date-str)]
    [doi date nil nil nil nil]))


(defn parse-doi-date-count [line]
  (let [[doi date-str count-str] (split line #"\t")
        date (format/parse date-str)
        cnt (. Integer parseInt count-str)]
    [doi date cnt nil nil nil]))


(defn parse-doi-count [line]
  (let [[doi count-str] (split line #"\t")
        cnt (. Integer parseInt count-str)]
    [doi nil cnt nil nil nil]))

(defn run
    "Import latest Laskuri output from an S3 bucket. Base is the directory within the bucket, usually a timestamp."
    [bucket base]
    (insert-event-from-type bucket base "ever-doi-first-date" "first-resolution" "CrossRefLogs" true parse-doi-date)
    (insert-event-from-type bucket base "ever-doi-count" "total-resolutions" "CrossRefLogs" true parse-doi-count)
    (insert-event-from-type bucket base "year-doi-period-count" "yearly-resolutions" "CrossRefLogs" true parse-doi-date-count)
    (insert-event-from-type bucket base "month-doi-period-count" "monthly-resolutions" "CrossRefLogs" true parse-doi-date-count)
    (insert-event-from-type bucket base "day-doi-period-count" "daily-resolutions" "CrossRefLogs" true parse-doi-date-count))
