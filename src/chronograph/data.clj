(ns chronograph.data
  (:require [chronograph.db :as d])
  (:require [clj-http.client :as client])
  (:require [crossref.util.config :refer [config]]
            [crossref.util.date :as crdate]
            [crossref.util.doi :as crdoi])
  (:require [korma.core :as k])
  (:require [clj-time.core :as t]
            [clj-time.periodic :as time-period])
  (:require [clj-time.coerce :as coerce]
            [clj-time.format :refer [parse formatter unparse]])
  (:require [robert.bruce :refer [try-try-again]]))

(def works-endpoint "http://api.crossref.org/v1/works")
(def api-page-size 1000)
(def yyyy-mm-dd (formatter (t/default-time-zone) "yyyy-MM-dd" "yyyy-MM-dd"))

(defn get-type-id-by-name [type-name]
  (:id (first (k/select d/types (k/where (= :ident type-name))))))

(defn get-source-id-by-name [source-name]
  (:id (first (k/select d/sources (k/where (= :ident source-name))))))

(defn get-doi-id [doi create?]
  (try
  (let [retrieved (first (k/select d/doi (k/where (= :doi doi))))]
    (if retrieved
      (:id retrieved)
      (when create? (let [inserted (k/insert d/doi (k/values {:doi doi}))]
        (:GENERATED_KEY inserted)))))
  ; Return nil on error
  (catch Exception e (prn "EXCEPTION" e))))

(defn insert-event [doi type-id source-id date overwrite cnt arg1 arg2 arg3]
  (let [doi-id (get-doi-id doi true)]
    (when overwrite (k/delete d/events (k/where {:doi doi-id
                                                :event (coerce/to-sql-time date)
                                                :source source-id
                                                :type type-id})))
    (try
    (k/insert d/events (k/values {:doi doi-id
                                  :type type-id
                                  :source source-id
                                  :event (coerce/to-sql-time date)
                                  :inserted (coerce/to-sql-time (t/now))
                                  :count (or cnt 1)
                                  :arg1 arg1 :arg2 arg2 :arg3 arg3}))
    (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-event-timeline
  "Insert parts of an DOI's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per DOI, source, type.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [doi type-id source-id data merge-fn]
  (let [doi-id (get-doi-id doi true)
        initial-row (first (k/select d/event-timelines
                                     (k/where (and
                                                (= :doi doi-id)
                                                (= :type type-id)
                                                (= :source source-id)))))
        initial-row-data (or (:timeline initial-row) {})
        merged-data (merge-with merge-fn initial-row-data data)]
    (if initial-row
      (k/update d/event-timelines
                (k/where {:doi doi-id
                          :type type-id
                          :source source-id})
                (k/set-fields {:timeline merged-data}))
      
      (k/insert d/event-timelines
                (k/values {:doi doi-id
                           :type type-id
                           :source source-id
                           :inserted (coerce/to-sql-time (t/now))
                           :timeline merged-data})))))

(defn insert-domain-timeline
  "Insert parts of a Referrer Domain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-id source-id data merge-fn]
  (let [initial-row (first (k/select d/referrer-domain-timelines
                                     (k/where (and
                                                (= :domain domain)
                                                (= :host host)
                                                (= :type type-id)
                                                (= :source source-id)))))
        initial-row-data (or (:timeline initial-row) {})
        merged-data (merge-with merge-fn initial-row-data data)]
    (if initial-row
      (k/update d/referrer-domain-timelines
                (k/where {:domain domain
                          :host host
                          :type type-id
                          :source source-id})
                (k/set-fields {:timeline merged-data}))
      
      (k/insert d/referrer-domain-timelines
                (k/values {:domain domain
                           :host host
                           :type type-id
                           :source source-id
                           :inserted (coerce/to-sql-time (t/now))
                           :timeline merged-data})))))

(defn insert-subdomain-timeline
  "Insert parts of a Referrer Subdomain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-id source-id data merge-fn]
  (let [initial-row (first (k/select d/referrer-subdomain-timelines
                                     (k/where (and
                                                (= :domain domain)
                                                (= :host host)
                                                (= :type type-id)
                                                (= :source source-id)))))
        initial-row-data (or (:timeline initial-row) {})
        merged-data (merge-with merge-fn initial-row-data data)]
    (if initial-row
      (k/update d/referrer-subdomain-timelines
                (k/where {:domain domain
                          :host host
                          :type type-id
                          :source source-id})
                (k/set-fields {:timeline merged-data}))
      
      (k/insert d/referrer-subdomain-timelines
                (k/values {:domain domain
                           :host host
                           :type type-id
                           :source source-id
                           :inserted (coerce/to-sql-time (t/now))
                           :timeline merged-data})))))

(defn sort-timeline-values
  "For a hashmap timeline, return as sorted list"
  [timeline]
  (into (sorted-map) (sort-by first t/before? (seq timeline))))

(defn get-doi-timelines
  "Get all timelines for a DOI"
  [doi]
  (when-let [timelines (k/select
    d/event-timelines
    (k/where {:doi (get-doi-id doi false)})
    (k/with d/types))]
    (map (fn [timeline]
           (assoc timeline :timeline (sort-timeline-values (:timeline timeline))))
         timelines)))

(defn get-domain-timelines
  "Get all timelines for a domain"
  [domain]
  (when-let [timelines (k/select
    d/referrer-domain-timelines
    (k/where {:host domain})
    (k/with d/types))]
    (map (fn [timeline]
           (assoc timeline :timeline (sort-timeline-values (:timeline timeline))))
         timelines)))

(defn get-subdomain-timelines
  "Get all timelines for a subdomain"
  [subdomain]
  (when-let [timelines (k/select
    d/referrer-domain-timelines
    (k/where {:host subdomain})
    (k/with d/types))]
    (map (fn [timeline]
           (assoc timeline :timeline (sort-timeline-values (:timeline timeline))))
         timelines)))

(defn insert-domain-event [host domain type-id source-id date overwrite cnt]
    (when overwrite (k/delete d/referrer-domain-events (k/where {:domain domain
                                                                :event (coerce/to-sql-time date)
                                                                :source source-id
                                                                :type type-id})))
    (try
    (k/insert d/referrer-domain-events (k/values {:host host
                                                  :domain domain
                                                  :type type-id
                                                  :source source-id
                                                  :event (coerce/to-sql-time date)
                                                  :inserted (coerce/to-sql-time (t/now))
                                                  :count (or cnt 1)}))
    (catch Exception e (prn "EXCEPTION" e))))

(defn insert-subdomain-event [host domain type-id source-id date overwrite cnt]
    (when overwrite (k/delete d/referrer-domain-events (k/where {:domain domain
                                                                 :host host
                                                                :event (coerce/to-sql-time date)
                                                                :source source-id
                                                                :type type-id})))
    (try
    (k/insert d/referrer-subdomain-events (k/values { :host host
                                                      :domain domain
                                                      :type type-id
                                                      :source source-id
                                                      :event (coerce/to-sql-time date)
                                                      :inserted (coerce/to-sql-time (t/now))
                                                      :count (or cnt 1)}))
    (catch Exception e (prn "EXCEPTION" e))))

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

(defn get-dois-updated-since [date]
  (let [base-q (if date 
          {:filter (str "from-update-date:" (unparse yyyy-mm-dd date))}
          {})
        results (try-try-again #(client/get (str works-endpoint \? (client/generate-query-string (assoc base-q :rows 0))) {:as :json}))
        num-results (-> results :body :message :total-results)
        ; Extra page just in case.
        num-pages (+ (quot num-results api-page-size) 2)
        page-queries (map #(str works-endpoint \? (client/generate-query-string (assoc base-q
                                                                                  :rows api-page-size
                                                                                  :offset (* api-page-size %)))) (range num-pages))
        
        issued-type-id (get-type-id-by-name "issued")
                  updated-type-id (get-type-id-by-name "updated")
                  metadata-source-id (get-source-id-by-name "CrossRefMetadata")]
    
      (doseq [page-query page-queries]
        (prn "Fetching" page-query)
        (let [response (try-try-again #(client/get page-query {:as :json}))
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
                  
                  ; updated
                  updatedInput (-> item :deposited :date-parts first)
                  updated (apply crdate/crossref-date updatedInput)
                  updatedDate (coerce/to-sql-date (crdate/as-date updated))]
              (insert-event the-doi issued-type-id metadata-source-id date true 1 issuedString nil nil)
              (insert-event the-doi updated-type-id metadata-source-id updatedDate true 1 nil nil nil))))
        (prn "Next"))))

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [the-doi]
  (let [url (crdoi/normalise-doi the-doi)
        result (try-try-again #(client/get url {:follow-redirects true :throw-exceptions false}))
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
  ; TODO only run since given date
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

(defn get-doi-facts
  "Get 'facts' (i.e. non-time-based events)"
  [the-doi]
  (when-let [doi (first (k/select d/doi (k/where (= :doi the-doi))))]
    (let [doi-id (:id doi)
          events (k/select d/events 
                 (k/with d/sources)
                 (k/with d/types)
                 (k/where (and (= :event nil) (= :doi doi-id)))
                 
                 (k/fields [:sources.name :source-name]
                            [:types.name :type-name]))]
  events)))

(defn get-doi-events 
  "Get 'events' (i.e. events with a date stamp)"
  [the-doi]
  (when-let [doi (first (k/select d/doi (k/where (= :doi the-doi))))]
    (let [doi-id (:id doi)
          events (k/select d/events 
                 (k/with d/sources)
                 (k/with d/types)
                 (k/where (and (not= :event nil) (= :doi doi-id)))
                 (k/order :events.event)
                 (k/fields [:sources.name :source-name]
                            [:types.name :type-name]))]
      events)))

(defn get-domain-events
  "Get domain 'events' (i.e. events with a date stamp)"
  [host]
  (let [events (k/select d/referrer-domain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (not= :event nil) (= :host host)))
               (k/order :referrer_domain_events.event)
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
    events))

(defn get-subdomain-events
  "Get subdomain 'events' (i.e. events with a date stamp)"
  [host]
  (let [events (k/select d/referrer-subdomain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (not= :event nil) (= :host host)))
               (k/order :referrer_subdomain_events.event)
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
    events))

(defn set-first-resolution-log [the-doi date]
  (k/exec-raw ["insert into doi (doi, firstResolutionLog) values (?, ?) on duplicate key update firstResolutionLog = ?"
               [the-doi (coerce/to-sql-date date) (coerce/to-sql-date date)]]))

(defn delete-events-for-type [type-name]
  (let [type-id (get-type-id-by-name type-name)]
    (prn "Delete events for type" type-name type-id)
    (k/delete d/events (k/where (= :type type-id)))))

(defn delete-domain-events-for-type [type-name]
  (let [type-id (get-type-id-by-name type-name)]
    (prn "Delete referrer domain events for type" type-name type-id)
    (k/delete d/referrer-domain-events (k/where (= :type type-id)))))

(defn delete-subdomain-events-for-type [type-name]
  (let [type-id (get-type-id-by-name type-name)]
    (prn "Delete referrer subdomain events for type" type-name type-id)
    (k/delete d/referrer-subdomain-events (k/where (= :type type-id)))))

(defn get-other-subdomains [host]
  ; Find mapping of host www.xyz.com to domain xyz
  (when-let [sample (first (k/select d/referrer-subdomain-timelines (k/where (= :host host)) (k/limit 1)))]
    (let [domain (:domain sample)
          other-subdomains (k/select d/referrer-subdomain-timelines
                                     (k/fields :host)
                                     (k/group :host)
                                     ; (k/aggregate (sum :count) :cnt)
                                     (k/where (= :domain domain))
                                     ; (k/order :cnt :desc)
                                     )]
      other-subdomains)))

(defn get-subdomains-for-domain-host [host]
    (when-let [sample (first (k/select d/referrer-domain-timelines (k/where (= :host host)) (k/limit 1)))]
    (let [domain (:domain sample)
          subdomains (k/select d/referrer-subdomain-timelines
                                     (k/fields :host)
                                     (k/group :host)
                                     ; (k/aggregate (sum :count) :cnt)
                                     (k/where (= :domain domain))
                                     ; (k/order :cnt :desc)
                                     )]
      subdomains)))

(defn time-range
  "Return a lazy sequence of DateTime's from start to end, incremented
  by 'step' units of time."
  [start end step]
  (let [inf-range (time-period/periodic-seq start step)
        below-end? (fn [tt] (t/within? (t/interval start end)
                                         tt))]
    (take-while below-end? inf-range)))

(defn interpolate-timeline
  [values first-date last-date step]
  (let [date-range (time-range first-date last-date step)]
    (map (fn [date] [date (or (get values date) 0)]) date-range)))