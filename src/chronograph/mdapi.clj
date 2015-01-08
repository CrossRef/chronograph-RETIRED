(ns chronograph.mdapi
  (:require [chronograph.data :as data]
            [chronograph.core :as core])
  (:require [clj-http.client :as client])
  (:require [crossref.util.config :refer [config]]
            [crossref.util.date :as crdate]
            [crossref.util.doi :as crdoi])
  (:require [korma.core :as k])
  (:require [clj-time.core :as t]
            [clj-time.periodic :as time-period])
  (:require [clj-time.coerce :as coerce]
            [clj-time.format :refer [parse formatter unparse]])
  (:require [robert.bruce :refer [try-try-again]])
  (:require [clojure.core.async :refer [<!! >!! go chan go-loop]])
  (:require [clojure.core.async.lab :refer [spool]]))

(def works-endpoint "http://api.crossref.org/v1/works")
(def api-page-size 1000)
(def num-workers 20)

(def page-channel (chan))
(def issued-type-id (data/get-type-id-by-name "issued"))
(def updated-type-id (data/get-type-id-by-name "updated"))
(def metadata-source-id (data/get-source-id-by-name "CrossRefMetadata"))

(defn fetch-and-parse [url]
  (prn "GET URL" url)
  (let [response (try-try-again {:sleep 5000 :tries :unlimited} #(client/get url {:as :json}))
        items (-> response :body :message :items)]
        (doseq [item items]
          (let [the-doi (:DOI item)
                ; list of date parts in CrossRef date format
                issued-input (-> item :issued :date-parts first)
                ; CrossRef date native format
                ; Issued may be missing, e.g. 10.1109/lcomm.2012.030912.11193
                ; Or may be a single null in a vector.
                issued-input-ok (and (not-empty issued-input) (every? number? issued-input))
                
                issued (when issued-input-ok (apply crdate/crossref-date issued-input))
                ; Nominal, but potentially lossily converted format.
                ; Coerce will work with the the DateTimeProtocol
                issuedDate (when issued-input-ok (coerce/to-sql-date (crdate/as-date issued)))
                ; Non-lossy, but string representation of date.
                issuedString (str issued)
                
                ; updated
                updatedInput (-> item :deposited :date-parts first)
                updated (apply crdate/crossref-date updatedInput)
                updatedDate (coerce/to-sql-date (crdate/as-date updated))]

            ; Insert in background, will take less time than the API fetch.
            ; Some issue dates are missing. Don't insert.
            (when issued-input-ok
              (go (data/insert-event the-doi issued-type-id metadata-source-id issuedDate 1 issuedString nil nil)))
            (go (data/insert-event the-doi updated-type-id metadata-source-id updatedDate 1 nil nil nil))))))

(dotimes [_ num-workers]
 (go-loop [url (<!! page-channel)]
   (when url
     (fetch-and-parse url)
     (recur (<!! page-channel)))))
   
(defn get-num-dois-updated-since-async [date]
  (let [base-q (if date 
          {:filter (str "from-update-date:" (unparse core/yyyy-mm-dd date))}
          {})
        results (try-try-again {:sleep 5000 :tries :unlimited} #(client/get (str works-endpoint \? (client/generate-query-string (assoc base-q :rows 0))) {:as :json}))
        num-results (-> results :body :message :total-results)
        ; Extra page just in case.
        num-pages (+ (quot num-results api-page-size) 2)
        page-queries (map #(str works-endpoint \? (client/generate-query-string (assoc base-q
                                                                                  :rows api-page-size
                                                                                  :offset (* api-page-size %)))) (range num-pages))]
        ; Spool, blocking.
        (doseq [url page-queries]
          (>!! page-channel url))))


(defn run-doi-extraction-new-updates []
  (let [last-run-date (data/get-last-run-date)
        now (t/now)]
    (get-num-dois-updated-since-async last-run-date)    
    (data/set-last-run-date! now)))

