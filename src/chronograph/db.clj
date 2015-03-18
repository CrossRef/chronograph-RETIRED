(ns chronograph.db
  (:require [crossref.util.config :refer [config]])
  (:require [korma.db :as kdb])
  (:require [korma.core :as k])
  (:require [korma.db :refer [mysql with-db defdb]])
  (:require [clj-time.coerce :as coerce])
  (:require [clojure.data.json :as json])
  (:require [clojure.edn :as edn]
            [clojure.java.io :refer [reader]])
  (:require [chronograph.types :as types])
  (:require [korma.sql.engine :as korma-engine]
            [korma.core :refer :all]))

(defdb db
  (mysql {:user (:database-username config)
          :password (:database-password config)
          :db (:database-name config)}))

; TODO MOVE TO DATA?
(defn shard-name
  [storage-format type-name]
  {:pre [(types/type-names type-name)
         (types/storage-formats storage-format)]}
  (let [type-name-safe (.replaceAll (name type-name) "[^a-zA-Z]" "")
        storage-format-safe (.replaceAll (name storage-format) "[^a-zA-Z]" "")]
    (str "shard_" type-name-safe "_" storage-format-safe)))

(defn ensure-shard-tables!
  "Ensure every shard table exists."
  []
  (doseq [typ types/types]
    (let [shard-table-name (shard-name (:storage typ) (:name typ))]
    (condp = (:storage typ)
      ; This is far from ideal, but it can't be done with prepared statements. 
      ; No chance of sql injection as these inputs are hard-coded. 
      :timeline (k/exec-raw [(str "create table if not exists " shard-table-name " like timeline_shard_template") []])
      :event (k/exec-raw [(str "create table if not exists " shard-table-name " like event_shard_template") []])
      :milestone (k/exec-raw [(str "create table if not exists " shard-table-name " like milestone_shard_template") []])
      :fact (k/exec-raw [(str "create table if not exists " shard-table-name " like fact_shard_template") []])
      ))))

(defn all-fact-tables
  "Return seq of all fact shard table names"
  []
  (let [event-types (filter #(= (:storage %) :fact) types/types)]
    (map #(shard-name (:storage %) (:name %)) event-types)))


(defn all-event-tables
  "Return seq of all event shard table names"
  []
  (let [event-types (filter #(= (:storage %) :event) types/types)]
    (map #(shard-name (:storage %) (:name %)) event-types)))

(defn all-milestone-tables
  "Return seq of all milestone shard table names"
  []
  (let [event-types (filter #(= (:storage %) :milestone) types/types)]
    (map #(shard-name (:storage %) (:name %)) event-types)))

(defn all-timeline-tables
  "Return seq of all timeline shard table names"
  []
  (let [event-types (filter #(= (:storage %) :timeline) types/types)]
    (map #(shard-name (:storage %) (:name %)) event-types)))

; TODO Above values could be stored. Probably wouldn't make much practical difference though.

(defn coerce-sql-date
  "Coerce from SQL date if not null"
  [date]
  (when date (coerce/from-sql-date date)))

(k/defentity state
             (k/entity-fields
               :name
               :theDate))

(k/defentity sources
  (k/pk :id)
  (k/entity-fields :id :ident :name))

(k/defentity types
  (k/pk :id)
  (k/entity-fields :id :ident :name :milestone :arg1desc :arg2desc :arg3desc))

; TODO RETIRE
(k/defentity events
  (k/pk :id)
  (k/entity-fields
    :id
    :doi
    :count
    :event
    :inserted
    :source
    :type
    :arg1
    :arg2
    :arg3)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :event (when-let [d (:event input)] (coerce-sql-date d)))))))

; These are used in chronograph.data to serialise and deserialise.
; Not included in a Korma entity here because the table name is dynamic.
(defn coerce-timeline-in [timeline]
  "Coerce a timeline to Java Date from Joda Date, so it can be serialized"
  (reduce-kv (fn [m k v] (assoc m (coerce/to-date k) v)) {} timeline))

(defn coerce-timeline-out [timeline]
  (reduce-kv (fn [m k v] (assoc m (coerce/from-date k) v)) {} timeline))

(defn coerce-event-out [event]
  (assoc event :event (coerce/from-sql-date (:event event))
               :inserted (coerce/from-sql-date (:inserted event))))

(k/defentity referrer-domain-timelines
  (k/table "referrer_domain_timelines")
  (k/pk :id)
  (k/entity-fields
    :id
    :domain
    :host
    :inserted
    :source
    :type
    :timeline)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/prepare
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (pr-str (coerce-timeline-in d)))))))
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (coerce-timeline-out (edn/read (java.io.PushbackReader. (reader d))))))))))

(k/defentity referrer-subdomain-timelines
  (k/table "referrer_subdomain_timelines")
  (k/pk :id)
  (k/entity-fields
    :id
    :domain
    :host
    :inserted
    :source
    :type
    :timeline)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/prepare
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (pr-str (coerce-timeline-in d)))))))
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (coerce-timeline-out (edn/read (java.io.PushbackReader. (reader d))))))))))

(k/defentity doi-domain-timelines
  (k/table "doi_domain_referral_timelines")
  (k/pk :id)
  (k/entity-fields
    :id
    :doi
    :host
    :source
    :type
    :inserted
    :timeline)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/prepare
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (pr-str (coerce-timeline-in d)))))))
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (coerce-timeline-out (edn/read (java.io.PushbackReader. (reader d))))))))))


(k/defentity doi-domain-month-timelines
  (k/table "doi_domain_referral_month_timelines")
  (k/pk :id)
  (k/entity-fields
    :id
    :doi
    :host
    :source
    :type
    :inserted
    :timeline
    :month)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/prepare
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (pr-str (coerce-timeline-in d)))))))
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :timeline (when-let [d (:timeline input)] (coerce-timeline-out (edn/read (java.io.PushbackReader. (reader d))))))))))

; Because Korma doesn't allow distinct on a subset of fields, we need to create a new entity.
(k/defentity doi-domain-month-timelines-base
  (k/table "doi_domain_referral_month_timelines")
  (k/pk :id)
  (k/entity-fields
    :doi
    :host))


(k/defentity referrer-domain-events
  (k/table "referrer_domain_events")
  (k/entity-fields
    :event
    :count
    :domain
    :source
    :type)
    (k/belongs-to sources {:fk :source})
    (k/belongs-to types {:fk :type})
    (k/transform
    (fn [input]
      (when input
        (assoc input
          :event (when-let [d (:event input)] (coerce-sql-date d)))))))

(k/defentity referrer-subdomain-events
  (k/table "referrer_subdomain_events")
  (k/entity-fields
    :event
    :count
    :domain
    :source
    :type)
  (k/belongs-to sources {:fk :source})
  (k/belongs-to types {:fk :type})
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :event (when-let [d (:event input)] (coerce-sql-date d)))))))


(k/defentity top-domains
  (k/table "top_domains")
  (k/pk :id)
  (k/entity-fields
    :id
    :domains
    :month)
  (k/prepare
    (fn [input]
      (when input
        (assoc input
          :domains (when-let [d (:domains input)] (pr-str d))))))
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :month (coerce/from-sql-date (:month input))
          :domains (when-let [d (:domains input)] (edn/read (java.io.PushbackReader. (reader d)))))))))


(k/defentity member-domains
  (k/table "member_domains")
  (k/pk :id)
  (k/entity-fields
    ["member_id" :member-id]
    :domain))


(k/defentity resolutions
  (k/table "resolutions")
  (k/pk :doi)
  (k/entity-fields
    :doi :resolved))

(k/defentity
  tokens
  (k/entity-fields
   :token
   [:allowed_sources :allowed-sources]
   [:allowed_types :allowed-types])

  (k/transform
    (fn [input]
      (when input
        (assoc input
          :allowed-sources (set (map keyword (.split (:allowed-sources input) ",")))
          :allowed-types (set (map keyword (.split (:allowed-types input) ","))))))))

(k/defentity heartbeat-bucket
  (k/table "heartbeat_bucket")
  (k/entity-fields
    ["bucket_date" :date]
    ["heartbeat_count" :heartbeat-count]
    ["push_count" :push-count]
    :type))
