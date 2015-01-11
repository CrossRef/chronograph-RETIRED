(ns chronograph.data
  (:require [chronograph.db :as d]
            [chronograph.core :as core]
            [chronograph.util :as util])
  (:require [clj-http.client :as client])
  (:require [crossref.util.config :refer [config]]
            [crossref.util.date :as crdate]
            [crossref.util.doi :as crdoi])
  (:require [korma.core :as k]
            [korma.db :as kdb])
  (:require [clj-time.core :as t]
            [clj-time.periodic :as time-period])
  (:require [clj-time.coerce :as coerce]
            [clj-time.format :refer [parse formatter unparse]])
  (:require [robert.bruce :refer [try-try-again]])
  (:require [clojure.core.async :as async :refer [<! <!! go chan]]))

(defn insert-member-domains [member-id domains]
  (kdb/transaction
    (doseq [domain domains]
      (k/exec-raw ["INSERT INTO member_domains (member_id, domain) VALUES (?, ?) ON DUPLICATE KEY UPDATE domain = domain"
                 [member-id domain]]))))

(defn get-domain-whitelist 
  "Load the whitelist file"
  []
  (with-open [reader (clojure.java.io/reader (clojure.java.io/resource "domain-whitelist.txt"))]
    (let [lines (line-seq reader)
          ; split into [+-? domain]
          lines (map (fn [line] [(.substring line 0 1) (.substring line 1)]) lines)
          ; turn example.com into example
          lines-domains (map (fn [[marker domain]] [marker (second (util/get-main-domain domain))]) lines)
          whitelist (filter #(= "+" (first %)) lines-domains)
          blacklist (filter #(= "-" (first %)) lines-domains)
          unsurelist (filter #(= "?" (first %)) lines-domains)
          
          whitelist-output (into #{} (map second whitelist))
          blacklist-output (into #{} (map second blacklist))
          unknownlist-output (into #{} (map second unsurelist))]
      [whitelist-output blacklist-output unknownlist-output])))

(def whitelists (get-domain-whitelist))

(def domain-whitelist (first whitelists))
(def domain-blacklist (second whitelists))
(def domain-unknownlist (nth whitelists 2))

(defn domain-whitelisted? [domain] (domain-whitelist domain))

(defn get-type-id-by-name [type-name]
  (:id (first (k/select d/types (k/where {:ident type-name})))))

(defn get-source-id-by-name [source-name]
  (:id (first (k/select d/sources (k/where {:ident source-name})))))

(defn insert-event [doi type-id source-id date cnt arg1 arg2 arg3]
  (try
    (k/exec-raw ["INSERT INTO events (doi, type, source, event, inserted, count, arg1, arg2, arg3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event = ?, count = ?, arg1 = ?, arg2 = ?, arg3 = ?"
                 [doi type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1) arg1 arg2 arg3
                  (coerce/to-sql-time date) (or cnt 1) arg1 arg2 arg3]])
    (catch Exception e (prn "EXCEPTION" e))))

(defn insert-domain-event [domain type-id source-id date cnt]
  (when (and (< (.length domain) 128))
    (try
      (k/exec-raw ["INSERT INTO referrer_domain_events (domain, type, source, event, inserted, count) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event = ?, count = ?"
                   [domain type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1)
                    (coerce/to-sql-time date) (or cnt 1)]])
      (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-subdomain-event [host domain type-id source-id date cnt]
  (when (and (< (.length domain) 128) (< (.length host) 128))
    (try
      (k/exec-raw ["INSERT INTO referrer_subdomain_events (subdomain, domain, type, source, event, inserted, count) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event = ?, count = ?"
                   [host domain type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1)
                    (coerce/to-sql-time date) (or cnt 1)]])
      (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-events-chunk
  "Insert chunk of inputs to insert-event"
  [chunk]
  (kdb/transaction
    (prn "chunk insert-doi-resolutions-count")
    (doseq [args chunk]
      (apply insert-event args))))

(defn insert-doi-resolutions-count
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-doi-resolutions-count")
    (doseq [[doi cnt] chunk]
      (insert-event doi type-id source-id nil cnt nil nil nil))))

(defn insert-doi-first-resolution
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-doi-first-resolution")
    (doseq [[doi date] chunk]
      (insert-event doi type-id source-id date nil nil nil nil))))

(defn insert-domain-count
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-domain-count")
    (doseq [[domain cnt] chunk]
      (insert-domain-event domain type-id source-id nil cnt))))

(defn insert-subdomain-count
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-domain-count")
    (doseq [[host domain cnt] chunk]
      (insert-subdomain-event host domain type-id source-id nil cnt))))

(defn insert-month-top-domains
  [chunk]
  (kdb/transaction
    (doseq [[date top-domains] chunk]
      (k/delete d/top-domains (k/where {:month (coerce/to-sql-date date)}))
      (k/insert d/top-domains (k/values {:month (coerce/to-sql-date date) :domains top-domains})))))

; Channel for queueing timeline updates.
(def event-timeline-chan (chan))

(defn insert-event-timeline
  "Insert parts of an DOI's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per DOI, source, type.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [doi type-id source-id data merge-fn]
  (try 
    (let [initial-row (first (k/select d/event-timelines
                                       (k/where {:doi doi
                                                 :type type-id
                                                  :source source-id})))
          initial-row-data (or (:timeline initial-row) {})
          merged-data (merge-with merge-fn initial-row-data data)]
      (if initial-row
        (k/update d/event-timelines
                  (k/where {:doi doi
                            :type type-id
                            :source source-id})
                  (k/set-fields {:timeline merged-data}))
        
        (k/insert d/event-timelines
                  (k/values {:doi doi
                             :type type-id
                             :source source-id
                             :inserted (coerce/to-sql-time (t/now))
                             :timeline merged-data}))))
    ; SQL exception will be logged at console.
    (catch Exception _)))

; Four handlers to take work from the channel.
; When this is being used as the Laskuri input, the input format (partitioned by DOI) ensures that we're not going to have concurrency problems.
(go
  (while true
    (let [row (<!! event-timeline-chan)]
      (apply insert-event-timeline row))))

(go
  (while true
    (let [row (<!! event-timeline-chan)]
      (apply insert-event-timeline row))))

(go
  (while true
    (let [row (<!! event-timeline-chan)]
      (apply insert-event-timeline row))))

(go
  (while true
    (let [row (<!! event-timeline-chan)]
      (apply insert-event-timeline row))))

(defn insert-event-timelines
  "Insert chunk of event timelines in a transaction."
  [chunk type-id source-id]
  (kdb/transaction
    (prn "insert-event-timelines")
    (doseq [[doi timeline] chunk]
      (insert-event-timeline doi type-id source-id timeline #(max %1 %2)))))

(defn insert-domain-timeline
  "Insert parts of a Referrer Domain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-id source-id data merge-fn]
  (when (and (< (.length domain) 128) (< (.length host) 128))
    (let [initial-row (first (k/select d/referrer-domain-timelines
                                       (k/where {:domain domain
                                                  :host host
                                                  :type type-id
                                                  :source source-id})))
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
                             :timeline merged-data}))))))

(defn insert-domain-timelines
  "Insert chunk of domain timelines in a transaction."
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-domain-timelines")
    (doseq [[domain timeline] chunk]
      ; TODO only the host (not the domain) is supplied in current data format.
      (insert-domain-timeline domain domain type-id source-id timeline #(max %1 %2)))))

(defn insert-subdomain-timeline
  "Insert parts of a Referrer Subdomain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-id source-id data merge-fn]
  (when (and (< (.length domain) 128) (< (.length host) 128))
    (let [initial-row (first (k/select d/referrer-subdomain-timelines
                                       (k/where {:domain domain
                                                 :host host
                                                 :type type-id
                                                 :source source-id})))
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
                             :timeline merged-data}))))))

(defn insert-subdomain-timelines
  "Insert chunk of subdomain timelines in a transaction."
  [chunk type-id source-id]
  (kdb/transaction
    (prn "chunk insert-subdomain-timelines")
    (doseq [[host domain timeline] chunk]
      (insert-subdomain-timeline host domain type-id source-id timeline #(max %1 %2)))))


(defn sort-timeline-values
  "For a hashmap timeline, return as sorted list"
  [timeline]
  (into (sorted-map) (sort-by first t/before? (seq timeline))))

(defn get-doi-timelines
  "Get all timelines for a DOI"
  [doi]
  (when-let [timelines (k/select
    d/event-timelines
    (k/where {:doi doi})
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
    d/referrer-subdomain-timelines
    (k/where {:host subdomain})
    (k/with d/types))]
    (map (fn [timeline]
           (assoc timeline :timeline (sort-timeline-values (:timeline timeline))))
         timelines)))

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

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [the-doi]
  (let [url (crdoi/normalise-doi the-doi)
        result (try-try-again {:sleep 5000 :tries :unlimited} #(client/get url {:follow-redirects true :throw-exceptions false}))
        redirects (:trace-redirects result)
        first-redirect (second redirects)
        last-redirect (last redirects)
        ok (= 200 (:status result))]
        (when ok
          [first-redirect last-redirect])))

(defn time-range
  "Return a lazy sequence of DateTimes from start to end, incremented
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

(def exclude-domains #{"no-referrer." "doi.org"})

(defn whitelist-domain [[domain-host _]]
  (let [true-domain (second (util/get-main-domain domain-host))]
    (domain-whitelist true-domain)))

(defn get-top-domains-ever
  "Get all the top-domains stats ever. Return as {domain months} where months spans entire range"
  [redact? include-members take-n]
  (let [all-results (k/select d/top-domains)
                        
        dates (sort t/before? (map :month all-results))
        first-date (first dates)
        last-date (last dates)
      
        ; limit to top n domains per month   
        ; transform to pairs of [month domains-sorted]
        sorted-domains (map (fn [entry]
                         [(:month entry) (reverse (sort-by second (:domains entry)))]) all-results)
        
        ; Now we need to take the union of all domains that appeared in the top-n in any given month.
        top-n-domains (into #{} (mapcat (fn [[_ domains]] (map first (take take-n (if include-members
                                                                                    domains
                                                                                    (filter whitelist-domain domains)))))
                                        sorted-domains))
        
        ; transform into [month domain count]
        transformed (mapcat (fn [[month domains]] (map (fn [[domain cnt]] [month domain cnt]) domains)) sorted-domains)
        
        ; remove un-useful domains, include only desired top-n domains
        ; filtered (if include-members
                   ; (remove #(exclude-domains (second %)) transformed)
                   ; (filter #(domain-whitelist (second (util/get-main-domain (second %)))) transformed))
        
        filtered (remove #(exclude-domains (second %)) transformed)
        
        filtered (filter #(top-n-domains (second %)) filtered)
        
        ; group into {domain => [month domain count]}
        by-domain (group-by second filtered)
        
        ; group into {domain => {month => count}}
        by-domain-map (into {}
                            (map (fn [[domain dates]]
                                   [domain (into {} (map (fn [[month domain cnt]]
                                                           [month cnt]) dates))])
                                 by-domain))
        
        ; interpolate dates to insert zeroes where there's no data
        interpolated (when (and first-date last-date)
                        (map (fn [[domain dates]] [domain (interpolate-timeline
                                                            dates
                                                            first-date
                                                            last-date
                                                            (t/months 1))]) by-domain-map))
        
        redacted (map (fn [[domain dates]]
                        (let [[_ true-domain _] (util/get-main-domain domain)]
                          [(if (domain-whitelist true-domain) domain "redacted") dates])) interpolated)]
          (if redact? redacted interpolated)))
  
; TODO for now not being used until the DOI denorm question is resolved.
; (defn run-doi-resolution []
;   ; TODO only run since given date
;   (let [dois (k/select d/doi (k/where (= nil :resolved)))]
;     (prn (count dois) "to resolve")
;     (doseq [doi-info dois]
      
;       (let [the-doi (:doi doi-info)
;             resolutions (get-resolutions the-doi)]
;         ; Resolutions may not work (that's the point).
;         (when resolutions
;           (let [[first-resolution ultimate-resolution] resolutions]
;             (k/update d/doi (k/where (= :doi the-doi))
;                                           (k/set-fields {:firstResolution first-resolution
;                                                          :ultimateResolution ultimate-resolution
;                                                          :resolved (coerce/to-sql-date (t/now))}))))))))

; (defn get-doi-info [the-doi]
;   (let [info (first (k/select d/doi (k/where (= :doi the-doi))))]
;   info))

(defn get-doi-facts
  "Get 'facts' (i.e. non-time-based events)"
  [doi]
  (let [events (k/select d/events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (= :event nil) (= :doi doi)))
               
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
events))

(defn get-doi-events 
  "Get 'events' (i.e. events with a date stamp)"
  [doi]
  (let [events (k/select d/events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (not= :event nil) (= :doi doi)))
               (k/order :events.event)
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
    events))

(defn get-domain-events
  "Get domain 'events' (i.e. events with a date stamp)"
  [host]
  (let [events (k/select d/referrer-domain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (not= :event nil) (= :domain host)))
               (k/order :referrer_domain_events.event)
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
    events))

(defn get-domain-facts
  "Get 'facts' (i.e. non-time-based events)"
  [host]
  (let [events (k/select d/referrer-domain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (= :event nil) (= :domain host)))
               
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
events))


(defn get-subdomain-events
  "Get subdomain 'events' (i.e. events with a date stamp)"
  [host]
  (let [events (k/select d/referrer-subdomain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (not= :event nil) (= :subdomain host)))
               (k/order :referrer_subdomain_events.event)
               (k/fields [:sources.name :source-name]
                          [:types.name :type-name]))]
    events))

(defn get-subdomain-facts
  "Get 'facts' (i.e. non-time-based events)"
  [host]
  (let [events (k/select d/referrer-subdomain-events 
               (k/with d/sources)
               (k/with d/types)
               (k/where (and (= :event nil) (= :subdomain host)))
               
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

(defn get-subdomains-for-domain [domain]
    (let [subdomains (k/select d/referrer-subdomain-timelines
                     (k/fields :host)
                     (k/group :host)
                     ; (k/aggregate (sum :count) :cnt)
                     (k/where (= :domain domain))
                     ; (k/order :cnt :desc)
                     )]
      subdomains))



