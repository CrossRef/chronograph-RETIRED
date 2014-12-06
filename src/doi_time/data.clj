(ns doi-time.data
  (:require [doi-time.db :as d])
  (:require [clj-http.client :as client])
  (:require [crossref.util.config :refer [config]]
            [crossref.util.date :as crdate]
            [crossref.util.doi :as crdoi])
  (:require [korma.core :as k])
  (:require [clj-time.core :as t])
  (:require [clj-time.coerce :as coerce]
            [clj-time.format :refer [parse formatter unparse]]))

(def works-endpoint "http://api.crossref.org/v1/works")
(def api-page-size 1000)
(def yyyy-mm-dd (formatter (t/default-time-zone) "yyyy-MM-dd" "yyyy-MM-dd"))

(defn get-last-run-date
  "Last date that the DOI import was run."
  []
  (or (-> (k/select d/state (k/where (= "last-run" :name))) first :theDate coerce/from-sql-date)
      (when-let [date (:start-date config)] (apply t/date-time date))
      (t/now)))

(defn set-last-run-date!
  [date]
  (k/delete d/state (k/where (= :name "last-run")))
  (k/insert d/state (k/values {:name "last-run" :theDate (coerce/to-sql-date date)})))


(defn insert-doi [the-doi issuedDate issuedString depositedDate]
  (k/exec-raw ["insert into doi (doi, issuedDate, issuedString, firstDepositedDate) values (?, ?, ?, ?) on duplicate key update issuedDate = ?, issuedString = ?, redepositedDate = ?"
               [the-doi issuedDate issuedString depositedDate issuedDate issuedString depositedDate]]))

(defn get-dois-updated-since [date]
  (let [base-q (if date 
          {:filter (str "from-update-date:" (unparse yyyy-mm-dd date))}
          {})
        results (client/get (str works-endpoint \? (client/generate-query-string (assoc base-q :rows 0))) {:as :json})
        num-results (-> results :body :message :total-results)
        ; Extra page just in case.
        num-pages (+ (quot num-results api-page-size) 2)
        page-queries (map #(str works-endpoint \? (client/generate-query-string (assoc base-q
                                                                                  :rows api-page-size
                                                                                  :offset (* api-page-size %)))) (range num-pages))]
    
      (doseq [page-query page-queries]
        (prn "Fetching" page-query)
        (let [response (client/get page-query {:as :json})
              items (-> response :body :message :items)]
          (prn "Fetched")
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
                  
                  ; deposited
                  depositedInput (-> item :deposited :date-parts first)
                  deposited (apply crdate/crossref-date depositedInput)
                  depositedDate (coerce/to-sql-date (crdate/as-date deposited))]
              (insert-doi the-doi issuedDate issuedString depositedDate))))
        (prn "Next"))))

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [the-doi]
  (let [url (crdoi/normalise-doi the-doi)
        result (client/get url {:follow-redirects true :throw-exceptions false})
        redirects (:trace-redirects result)
        first-redirect (second redirects)
        last-redirect (last redirects)
        ok (= 200 (:status result))]
        (when ok
          [first-redirect last-redirect])))
  
(defn run-doi-extraction-new-updates []
  (let [last-run-date (get-last-run-date)
        now (t/now)]
    (get-dois-updated-since last-run-date)    
    (set-last-run-date! now)))

(defn run-doi-extraction-ever []
    (get-dois-updated-since nil))

(defn run-doi-resolution []
  ; TODO ONLY SINCE!!
  (let [dois (k/select d/doi (k/where (= nil :resolved)))]
    (prn (count dois) "to resolve")
    (doseq [doi-info dois]
      
      (let [the-doi (:doi doi-info)
            resolutions (get-resolutions the-doi)]
        ; Resolutions may not work (that's the point).
        (when resolutions
          (let [[first-resolution ultimate-resolution] resolutions]
            (k/update d/doi (k/where (= :doi the-doi))
                                          (k/set-fields {:firstResolution first-resolution
                                                         :ultimateResolution ultimate-resolution
                                                         :resolved (coerce/to-sql-date (t/now))}))))))))

(defn get-doi-info [the-doi]
  (let [info (first (k/select d/doi (k/where (= :doi the-doi))))]
  info))

(defn set-first-resolution-log [the-doi date]
  (k/exec-raw ["insert into doi (doi, firstResolutionLog) values (?, ?) on duplicate key update firstResolutionLog = ?"
               [the-doi (coerce/to-sql-date date) (coerce/to-sql-date date)]]))

