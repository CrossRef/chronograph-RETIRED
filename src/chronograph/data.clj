(ns chronograph.data
  (:require [chronograph.db :as d]
            [chronograph.core :as core]
            [chronograph.util :as util]
            [chronograph.types :as types])
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
  (:require [clojure.core.async :as async :refer [<! <!! >!! go chan]]
            [clojure.java.io :refer [reader]]
            [clojure.edn :as edn]))

; Types and sources by their database id.
; Set by init!
(def types-by-id (atom {}))
(def sources-by-id (atom {}))
(def type-ids-by-name (atom {}))
(def source-ids-by-name (atom {}))

(defn insert-member-domains [member-id domains]
  (kdb/transaction
    (doseq [domain domains]
      (prn "insert" member-id (count domains))
      (k/exec-raw ["INSERT INTO member_domains (member_id, domain) VALUES (?, ?) ON DUPLICATE KEY UPDATE domain = domain"
                 [member-id domain]]))))

(defn get-domain-override
  "Load the domain override file (claimed publisher domains that shouldnt' be treated as such)"
  []
  (with-open [reader (clojure.java.io/reader (clojure.java.io/resource "domain-override.txt"))]
    (let [lines (line-seq reader)
          domain-list (into #{} lines)]
      domain-list)))

(defn get-member-domains []
  (let [overrides (get-domain-override)
        domains (into #{} (map #(-> % :domain .toLowerCase) (k/select d/member-domains)))]
    (into #{} (remove overrides domains))))

(def member-domains (get-member-domains))

(defn domain-whitelisted? [domain] (not (member-domains domain)))

(defn get-shard-table-name-from-type-name
  [type-name]
  "Take type-name and return shard table name"
  (let [storage-format (-> types/types-by-name type-name :storage)
        shard-table-name (d/shard-name storage-format type-name)]
    shard-table-name))

(defn get-shard-info
  "Take type-name and source-name and return triplet of [shard-table-name type-id source-id]
  Packaged for frequent use."
  [type-name source-name]
  (let [typ (-> types/types-by-name type-name)
        type-id (@type-ids-by-name type-name)
        storage-format (:storage typ)
        shard-table-name (d/shard-name storage-format type-name)
        source-id (@source-ids-by-name source-name)
        conflict-resolution-method (:conflict typ)]
    [shard-table-name type-id source-id conflict-resolution-method]))


(defn decorate-events
  "Take a seq of events, decorate with type and source info"
  [events]
  (let [mapped (map
                 (fn [event] (assoc event :type (@types-by-id (:type event))
                                           :source (@sources-by-id (:source event))))
                 events)]
    mapped))

(defn insert-event
  "Insert event. No such thing as a duplicate."
  [doi type-name source-name date cnt arg1 arg2 arg3]
  ; Discard the conflict resolution method as they don't apply for events.
  (let [[table-name type-id source-id _] (get-shard-info type-name source-name)
        doi (crdoi/non-url-doi doi)]
    (try
      (k/exec-raw [(str "INSERT INTO " table-name " (doi, type, source, event, inserted, count, arg1, arg2, arg3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                   [doi type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1) arg1 arg2 arg3]])
      (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-milestone
  "Insert milestone using type's conflict resolution strategy with regard to the event dates."
  [doi type-name source-name date cnt arg1 arg2 arg3]
  (let [[table-name type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)
        doi (crdoi/non-url-doi doi)]
    (try
      (condp = conflict-resolution-method
        :older (k/exec-raw [(str "INSERT INTO " table-name " (doi, count, event, inserted, source, type, arg1, arg2, arg3) values (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
                                                            count = IF(event<VALUES(event), count, VALUES(count)),
                                                            event = IF(event<VALUES(event), event, VALUES(event)),
                                                            inserted = IF(event<VALUES(event), inserted, VALUES(inserted)),
                                                            source = IF(event<VALUES(event), source, VALUES(source)),
                                                            type = IF(event<VALUES(event), type, VALUES(type)),
                                                            arg1 = IF(event<VALUES(event), arg1, VALUES(arg1)),
                                                            arg2 = IF(event<VALUES(event), arg2, VALUES(arg2)),
                                                            arg3 = IF(event<VALUES(event), arg3, VALUES(arg3));)")
                   [doi type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1) arg1 arg2 arg3]])
        :newer (k/exec-raw [(str "INSERT INTO " table-name " (doi, count, event, inserted, source, type, arg1, arg2, arg3) values (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
                                                            count = IF(event>VALUES(event), count, VALUES(count)),
                                                            event = IF(event>VALUES(event), event, VALUES(event)),
                                                            inserted = IF(event>VALUES(event), inserted, VALUES(inserted)),
                                                            source = IF(event>VALUES(event), source, VALUES(source)),
                                                            type = IF(event>VALUES(event), type, VALUES(type)),
                                                            arg1 = IF(event>VALUES(event), arg1, VALUES(arg1)),
                                                            arg2 = IF(event>VALUES(event), arg2, VALUES(arg2)),
                                                            arg3 = IF(event>VALUES(event), arg3, VALUES(arg3));)")
                   [doi type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1) arg1 arg2 arg3]])
        :replace (k/exec-raw [(str "INSERT INTO " table-name " (doi, count, event, inserted, source, type, arg1, arg2, arg3) values (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
                                                            count = VALUES(count),
                                                            event = VALUES(event),
                                                            inserted = VALUES(inserted),
                                                            source = VALUES(source),
                                                            type = VALUES(type),
                                                            arg1 = VALUES(arg1),
                                                            arg2 = VALUES(arg2),
                                                            arg3 = VALUES(arg3);)")
                   [doi type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1) arg1 arg2 arg3]]))
      (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-fact
  "Insert fact using type's conflict resolution strategy with regard to the insertion date."
  [doi type-name source-name cnt arg1 arg2 arg3]
  (let [date (t/now)
        [table-name type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)
        doi (crdoi/non-url-doi doi)]
    (try
      (condp = conflict-resolution-method
        :older (k/exec-raw [(str "INSERT INTO " table-name " (doi, type, source, inserted, count, arg1, arg2, arg3) VALUES (?, ?,  ?, ?, ?, ?, ?, ?)
                                                           ON DUPLICATE KEY UPDATE
                                                           source = IF (inserted<VALUES(inserted), inserted, VALUES(inserted),
                                                           inserted = IF (inserted<VALUES(inserted), inserted, VALUES(inserted)),
                                                           count = IF (inserted<VALUES(inserted), count, VALUES(count)),
                                                           arg1 = IF (inserted<VALUES(inserted), arg1, VALUES(arg1)),
                                                           arg2 = IF (inserted<VALUES(inserted), arg2, VALUES(arg2)),
                                                           arg3 = IF (inserted<VALUES(inserted), arg3, VALUES(arg3))")
                   [doi type-id source-id (coerce/to-sql-time date) (or cnt 1) arg1 arg2 arg3]])
        
        
        :newer (k/exec-raw [(str "INSERT INTO " table-name " (doi, type, source, inserted, count, arg1, arg2, arg3) VALUES (?, ?,  ?, ?, ?, ?, ?, ?)
                                                           ON DUPLICATE KEY UPDATE
                                                           source = IF (inserted>VALUES(inserted), inserted, VALUES(inserted),
                                                           inserted = IF (inserted>VALUES(inserted), inserted, VALUES(inserted)),
                                                           count = IF (inserted>VALUES(inserted), count, VALUES(count)),
                                                           arg1 = IF (inserted>VALUES(inserted), arg1, VALUES(arg1)),
                                                           arg2 = IF (inserted>VALUES(inserted), arg2, VALUES(arg2)),
                                                           arg3 = IF (inserted>VALUES(inserted), arg3, VALUES(arg3))")
                   [doi type-id source-id (coerce/to-sql-time date) (or cnt 1) arg1 arg2 arg3]])
        
        
        :replace (k/exec-raw [(str "INSERT INTO " table-name " (doi, type, source, inserted, count, arg1, arg2, arg3) VALUES (?, ?,  ?, ?, ?, ?, ?, ?)
                                                             ON DUPLICATE KEY UPDATE
                                                             source = VALUES(source),
                                                             inserted = VALUES(inserted),
                                                             count = VALUES(count),
                                                             arg1 = VALUES(arg1),
                                                             arg2 = VALUES(arg2),
                                                             arg3 = VALUES(arg3)")
                   [doi type-id source-id (coerce/to-sql-time date) (or cnt 1) arg1 arg2 arg3]]))
      (catch Exception e (prn "EXCEPTION" e)))))


; Large buffer in case we are blocked on table write as the table is ISAM and might be locked.
; Don't allow timeline because it's not relevant (merge function etc required).
(def insert-event-channel (chan 10000))
(def insert-milestone-channel (chan 10000))
(def insert-fact-channel (chan 10000))

(defn insert-event-async
  "As insert-event but async. Block if buffer is full."
  [doi type-name source-name date cnt arg1 arg2 arg3]
  (prn "insert-event-async")
    (>!! insert-event-channel [doi type-name source-name date cnt arg1 arg2 arg3]))

(defn insert-milestone-async
  "As insert-milestone but async. Block if buffer is full."
  [doi type-name source-name date cnt arg1 arg2 arg3]
    (>!! insert-milestone-channel [doi type-name source-name date cnt arg1 arg2 arg3]))

(defn insert-fact-async
  "As insert-fact but async. Block if buffer is full."
  [doi type-name source-name cnt arg1 arg2 arg3]
    (>!! insert-fact-channel [doi type-name source-name cnt arg1 arg2 arg3]))

(go
  (while true
    (let [row (<! insert-event-channel)]
      (apply insert-event row))))

(go
  (while true
    (let [row (<! insert-milestone-channel)]
      (apply insert-milestone row))))

(go
  (while true
    (let [row (<! insert-fact-channel)]
      (apply insert-fact row))))

(defn insert-domain-event
  [domain type-id source-id date cnt]
    (when (and (< (.length domain) 128))
      (try
        (k/exec-raw ["INSERT INTO referrer_domain_events (domain, type, source, event, inserted, count) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event = ?, count = ?"
                     [domain type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1)
                      (coerce/to-sql-time date) (or cnt 1)]])
        (catch Exception e (prn "EXCEPTION" e)))))

(defn insert-subdomain-event
  [host domain type-id source-id date cnt]
  (when (and (< (.length domain) 128) (< (.length host) 128))
    (try
      (k/exec-raw ["INSERT INTO referrer_subdomain_events (subdomain, domain, type, source, event, inserted, count) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event = ?, count = ?"
                   [host domain type-id source-id (coerce/to-sql-time date) (coerce/to-sql-time (t/now)) (or cnt 1)
                    (coerce/to-sql-time date) (or cnt 1)]])
      (catch Exception e (prn "EXCEPTION" e)))))

; The following functions call insert-event, so carry through the type-name and source-name without resolving them.

(defn insert-events-chunk
  "Insert chunk of inputs to insert-event"
  [chunk]
  (kdb/transaction
    (prn "chunk insert-events-chunk")
    (doseq [args chunk]
      (apply insert-event args))))

(defn insert-facts-chunk
  "Insert chunk of inputs to insert-event"
  [chunk]
  (kdb/transaction
    (prn "chunk insert-facts-chunk")
    (doseq [args chunk]
      (apply insert-fact args))))

(defn insert-milestones-chunk
  "Insert chunk of inputs to insert-event"
  [chunk]
  (kdb/transaction
    (prn "chunk insert-milestones-chunk")
    (doseq [args chunk]
      (apply insert-milestone args))))

(defn insert-doi-resolutions-count
  [chunk type-name source-name]
    (kdb/transaction
      (prn "chunk insert-doi-resolutions-count")
      (doseq [[doi cnt] chunk]
        (insert-fact doi type-name source-name cnt nil nil nil))))

(defn insert-doi-first-resolution
  [chunk type-name source-name]
    (kdb/transaction
      (prn "chunk insert-doi-first-resolution")
      (doseq [[doi date] chunk]
        (insert-milestone doi type-name source-name date nil nil nil nil))))

(defn insert-domain-count
  [chunk type-name source-name]
  (let [[table-name type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)]
    ; Not sharded for domains (yet).
    (kdb/transaction
      (prn "chunk insert-domain-count")
      (doseq [[domain cnt] chunk]
        (insert-domain-event domain type-id source-id nil cnt)))))

(defn insert-subdomain-count
  [chunk type-name source-name]
  (let [[table-name type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)]
    ; Not sharded for domains (yet).
    (kdb/transaction
      (prn "chunk insert-domain-count")
      (doseq [[[host domain] cnt] chunk]
        (insert-subdomain-event host domain type-id source-id nil cnt)))))

(defn insert-month-top-domains
  [chunk]
  (kdb/transaction
    (doseq [[date top-domains] chunk]
      (k/delete d/top-domains (k/where {:month (coerce/to-sql-date date)}))
      (k/insert d/top-domains (k/values {:month (coerce/to-sql-date date) :domains top-domains})))))

(defn insert-events-chunk-type-source
  "Insert chunk of events of [doi date cnt arg1 arg2 arg3] "
  [chunk type-name source-name]
    (kdb/transaction
      (prn "chunk insert-events-chunk-type-source")
      (doseq [[doi date cnt arg1 arg2 arg3] chunk]
        (insert-event doi type-name source-name date cnt arg1 arg2 arg3))))

(defn read-edn [text]
  (when text
    (edn/read (java.io.PushbackReader. (reader text)))))

(defn insert-doi-timeline
  "Insert parts of an DOI's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per DOI, source, type.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [doi type-name source-name data merge-fn]
  (let [[table-name type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)
        doi (crdoi/non-url-doi doi)]
   (try 
      (let [initial-row (first (k/select table-name
                                         (k/where {:doi doi
                                                   :type type-id
                                                    :source source-id})))
            initial-row-data (d/coerce-timeline-out (or (read-edn (:timeline initial-row)) {}))
            merged-data (merge-with merge-fn initial-row-data data)]
        (if initial-row
          (k/update table-name
                    (k/where {:doi doi
                              :type type-id
                              :source source-id})
                    (k/set-fields {:timeline (pr-str (d/coerce-timeline-in merged-data))}))
          
          (k/insert table-name
                    (k/values {:doi doi
                               :type type-id
                               :source source-id
                               :inserted (coerce/to-sql-time (t/now))
                               :timeline (pr-str (d/coerce-timeline-in merged-data))}))))
      ; SQL exception will be logged at console.
      (catch Exception _))))

(defn insert-doi-timelines
  "Insert chunk of event timelines in a transaction."
  [chunk type-name source-name]
  (kdb/transaction
    (prn "insert-doi-timelines")
    (doseq [[doi timeline] chunk]
      (insert-doi-timeline doi type-name source-name timeline #(max %1 %2)))))

(defn insert-domain-timeline
  "Insert parts of a Referrer Domain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-name source-name data merge-fn]
  ; For domains use a single (non-sharded) table as the index load is far far lighter.
  (let [[_ type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)]
    (when (and (< (.length domain) 128) (< (.length host) 128))
      (let [initial-row (first (k/select d/referrer-domain-timelines
                                         (k/where {:domain domain
                                                    :host host
                                                    :type type-id
                                                    ; TODO should source be used to select? Is it part of identity?
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
                               :timeline merged-data})))))))

(defn insert-domain-timelines
  "Insert chunk of domain timelines in a transaction."
  [chunk type-name source-name]
  (kdb/transaction
    (prn "chunk insert-domain-timelines")
    (doseq [[domain timeline] chunk]
      ; TODO only the host (not the domain) is supplied in current data format.
      ; TODO merge function should be taken from the types registry.
      (insert-domain-timeline domain domain type-name source-name timeline #(max %1 %2)))))

(defn insert-doi-domain-timeline
  "Insert parts of a DOI / Domain event timeline. Overwrite at month level."
  [doi host type-name source-name data]
  (when (and (< (.length host) 128) (< (.length host) 128))      
    (let [[_ type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)
          
          ; Chop timeline into month chunks and insert each (overwriting).
          partitioned-by-month (group-by (fn [[date value]]
                                           (t/date-time (t/year date) (t/month date))) data)
          
          ; Convert each timeline form a seq of vectors into a map
          timelines (into {} (map (fn [[date timeline]] [date (into {} timeline)]) partitioned-by-month))]
        
      (doseq [[year-month timeline] timelines]   
        (k/exec-raw ["insert into doi_domain_referral_month_timelines (doi, host, type, month, source, inserted, timeline)
                     values (?, ?, ?, ?, ?, ?, ?) on duplicate key update timeline = values(timeline)"
                     [doi host type-id (coerce/to-sql-time year-month) source-id (coerce/to-sql-time (t/now))
                      (pr-str (d/coerce-timeline-in timeline))]])))))

(defn insert-doi-domain-timelines
  "Insert chunk of doi-domain timelines in a transaction."
  [chunk type-name source-name]
  (kdb/transaction
    (prn "chunk insert-doi-domain-timelines")
    (doseq [[[doi host] timeline] chunk]
      (insert-doi-domain-timeline doi host type-name source-name timeline))))

(defn insert-subdomain-timeline
  "Insert parts of a Referrer Subdomain's event timeline. This will merge the existing data.
  This should be used to update large quantities of data per Domain.
  merge-fn is used to replace duplicates. It should accept [old, new] and return new. E.g.  #(max %1 %2)"
  [host domain type-name source-name data merge-fn]
  ; For domains use a single (non-sharded) table as the index load is far far lighter.
  (let [[_ type-id source-id conflict-resolution-method] (get-shard-info type-name source-name)]
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
                               :timeline merged-data})))))))

(defn insert-subdomain-timelines
  "Insert chunk of subdomain timelines in a transaction."
  [chunk type-name source-name]
    (kdb/transaction
      (prn "chunk insert-subdomain-timelines")
      (doseq [[[host domain] timeline] chunk]
        (insert-subdomain-timeline host domain type-name source-name timeline #(max %1 %2)))))


(defn sort-timeline-values
  "For a hashmap timeline, return as sorted list"
  [timeline]
  (into (sorted-map) (sort-by first t/before? (seq timeline))))

(defn get-doi-timelines-for-table
  "Get all timelines for a DOI from named table"
  [doi table-name]
  (when-let [timelines (k/select table-name
                        (k/where {:doi (crdoi/non-url-doi doi)}))]    
                        (map (fn [timeline]
                               (assoc timeline :timeline (sort-timeline-values (d/coerce-timeline-out (read-edn (:timeline timeline))))
                                               :inserted (coerce/from-sql-time (:inserted timeline))))
                             timelines)))

(defn get-doi-timelines
  "Get all timelines for a DOI from all tables"
  [doi]
  (let [doi (crdoi/non-url-doi doi)
        tables (d/all-timeline-tables)
        all-events (apply concat (map #(get-doi-timelines-for-table doi %) tables))]
    (decorate-events all-events)))

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

(defn get-doi-domain-timelines
  "Get all timelines for a DOI / domain referral"
  [doi domain]
  ; Get timelines, more than one per month per type, as they're partitioned by month.
  ; (Really there's only one type).
  (when-let [timelines (k/select
                          d/doi-domain-month-timelines
                          (k/where {:host domain :doi doi})
                          (k/with d/types))]
    
    ; Group into month partitions per-type.
    (let [by-type (group-by :type timelines)
          merged-by-type (map (fn [[type-id timelines]]
                                ; Use the first item as a template and merge the respective timeline fragments into it.
                                (assoc (first timelines) :timeline
                                  (apply merge (map :timeline timelines)))) by-type)]
    (map (fn [timeline]
           (assoc timeline :timeline (sort-timeline-values (:timeline timeline))))
         merged-by-type))))

(defn get-available-doi-domain-timelines-for-doi
  "For a DOI return domains for all timelines"
  [doi  offset limit]
  (map :host
    (k/select
      d/doi-domain-timelines
      (k/where {:doi doi})
      (k/offset offset)
      (k/limit limit)
      (k/fields [:host]))))

(defn get-available-doi-domain-timelines-for-domain
  "For a domain return domains for all timelines"
  [host offset limit]
  (map :doi
    (k/select
      d/doi-domain-timelines
      (k/where {:host host})
      (k/offset offset)
      (k/limit limit)
      (k/fields [:doi]))))

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

(defn time-range
  "Return a lazy sequence of DateTimes from start to end, incremented
  by 'step' units of time, always inclusive of bounds."
  [start end step]
  (if (= start end)
    [start]
    (let [inf-range (time-period/periodic-seq start step)
          below-end? (fn [tt] (t/within? (t/interval start end)
                                           tt))]
      (concat (take-while below-end? inf-range) [end]))))
  
(defn interpolate-timeline
  [values first-date last-date step]
  (let [date-range (time-range first-date last-date step)]
    (map (fn [date] [date (or (get values date) 0)]) date-range)))

(def special-domains #{"no-referrer." "doi.org"})

(defn whitelist-domain [[domain-host _]]
    (not (member-domains domain-host)))

(defn get-top-domains-ever
  "Get all the top-domains stats ever. Return as {domain months} where months spans entire range"
  [redact? include-members take-n include-special]
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
                
        ; We may want not to exclude the special domains.
        filtered (if include-special
                   transformed
                   (remove #(special-domains (second %)) transformed))
        
        ; We have loads of domains. Only include those that we identified as the top-n.
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
                          [(if (not (member-domains domain)) domain "member domain") dates]) interpolated)]
          (if redact? redacted interpolated)))


(defn get-doi-facts
  "Get 'facts' (i.e. non-time-based events)"
  [doi]
  (let [doi (crdoi/non-url-doi doi)
        all-tables (d/all-fact-tables)
        all-events (map #(k/select %
                     (k/where (= :doi doi))) all-tables)
        all (apply concat all-events)
        exported (map d/coerce-event-out all)]
    (decorate-events exported)))

(defn get-doi-events 
  "Get 'events' (i.e. events with a date stamp)"
  [doi]
  (let [doi (crdoi/non-url-doi doi)
        all-tables (d/all-event-tables)
        all-events (map #(k/select %
                     (k/where (= :doi doi))) all-tables)
        all (apply concat all-events)
        exported (map d/coerce-event-out all)]
    (decorate-events exported)))

(defn get-doi-milestones 
  "Get 'events' (i.e. events with a date stamp)"
  [doi]
  (let [doi (crdoi/non-url-doi doi)
        all-tables (d/all-milestone-tables)
        all-events (map #(k/select %
                     (k/where (= :doi doi))) all-tables)
        all (apply concat all-events)
        exported (map d/coerce-event-out all)]
    (decorate-events exported)))

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
(defn set-first-resolution-log [doi date]
  (k/exec-raw ["insert into doi (doi, firstResolutionLog) values (?, ?) on duplicate key update firstResolutionLog = ?"
               [(crdoi/non-url-doi doi) (coerce/to-sql-date date) (coerce/to-sql-date date)]]))

(defn get-subdomains-for-domain [domain with-count?]
    (let [count-type (@type-ids-by-name :total-referrals-subdomain)
          subdomains (k/select d/referrer-subdomain-timelines
                     ; (k/fields :host)
                     (k/group :host)
                     ; (k/aggregate (sum :count) :cnt)
                     (k/where (= :domain domain))
                     ; (k/order :cnt :desc)
                     )
          ; TODO could this be a join?
          with-count (map (fn [subdomain]
                             (assoc subdomain :count
                                            (-> (k/select d/referrer-subdomain-events
                                                  (k/where {:subdomain (:host subdomain)
                                                            :type count-type}))
                                                first :count))) subdomains)] 
      (if with-count with-count subdomains)))

(defn get-tokens
  "Get all tokens as mapping of {token {:allowed-types #{} :allowed-sources #{}}.
  There are only going to be a small handful."
  []
  (let [types (k/select d/tokens)]
    (into {} (map (fn [item] [(:token item) item]) types))))

(defn get-recent-events
  "Get recent events ordered by event date"
  [type-name offset limit]
  
  (let [shard-table-name (get-shard-table-name-from-type-name type-name)
        events (k/select shard-table-name
                     (k/order :event :desc)
                     (k/offset offset)
                     (k/limit limit))
        decorated (decorate-events events)]
    decorated))

; Same implementation but good to keep separate.
; recent-facts and recent-timelines aren't meaningful.
(def get-recent-milestones get-recent-events)
  
(defn table-count
  "Return size of given table name"
  [table-name]
  (:cnt (first (k/select table-name (k/aggregate (count :*) :cnt)))))

(defn type-table-count
  "Return size of shard table by type name"
  [type-name]
  (let [shard-table-name (get-shard-table-name-from-type-name type-name)]
    (table-count shard-table-name)))

(defn init!
  "Stuff that needs to run before anything else."
  []
  ; Ensure that the relevant shard tables exist.
  (d/ensure-shard-tables!)
  (let [typs-by-id (into {} (map (fn [typ] (let [id (-> (k/select d/types (k/where {:ident (name (:name typ))})) first :id)] [id typ])) types/types))
        srcs-by-id (into {} (map (fn [src] (let [id (-> (k/select d/sources (k/where {:ident (name (:name src))})) first :id)] [id src])) types/sources))
        
        typ-ids-by-name (into {} (map (fn [[id typ]] [(:name typ) id]) typs-by-id))
        srcs-ids-by-name (into {} (map (fn [[id src]] [(:name src) id]) srcs-by-id))]
    (reset! types-by-id typs-by-id)
    (reset! sources-by-id srcs-by-id)
  
    (reset! type-ids-by-name typ-ids-by-name)
    (reset! source-ids-by-name srcs-ids-by-name)))

(defn truncate-date-for-bucket [date]
  (t/date-time (t/year date) (t/month date) (t/day date) (t/hour date)))

(defn get-heartbeat-bucket [type-name date]
  ; Truncate to hour
  
  (let [date-truncated (truncate-date-for-bucket date)
        result (k/select d/heartbeat-bucket (k/where {:type (@type-ids-by-name type-name) :bucket_date (coerce/to-sql-time date-truncated)}))]
  
      ; Return both so we know what the truncated date is.
    {:date date-truncated :heartbeat-count (or (-> result first :heartbeat-count) 0) :push-count (or (-> result first :push-count) 0)}))
  
  
(defn get-recent-heartbeats [type-name]
  (let [now (t/now)
        hours (map #(t/minus now (t/hours %)) (range 24))
        values (map (fn [hour] (get-heartbeat-bucket type-name hour)) hours)]
    values))

(defn inc-push-bucket [type-name]
  (let [now (truncate-date-for-bucket (t/now))]
  (k/exec-raw ["INSERT INTO heartbeat_bucket SET push_count = 1, bucket_date = ?, type = ? ON DUPLICATE KEY UPDATE push_count = push_count + 1"
               [(coerce/to-sql-time now) (@type-ids-by-name type-name)]])))

(defn inc-heartbeat-bucket [type-name]
  (let [now (truncate-date-for-bucket (t/now))]
  (k/exec-raw ["INSERT INTO heartbeat_bucket SET heartbeat_count = 1, bucket_date = ?, type = ? ON DUPLICATE KEY UPDATE heartbeat_count = heartbeat_count + 1"
               [(coerce/to-sql-time now) (@type-ids-by-name type-name)]])))
