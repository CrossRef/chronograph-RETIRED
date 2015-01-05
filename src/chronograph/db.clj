(ns chronograph.db
  (:require [crossref.util.config :refer [config]])
  (:require [korma.db :as kdb])
  (:require [korma.core :as k])
  (:require [korma.db :refer [mysql with-db defdb]])
  (:require [clj-time.coerce :as coerce])
  (:require [clojure.data.json :as json])
  (:require [clojure.edn :as edn]
            [clojure.java.io :refer [reader]])
  (:require [korma.sql.engine :as korma-engine]
            [korma.core :refer :all]))

(defdb db
  (mysql {:user (:database-username config)
          :password (:database-password config)
          :db (:database-name config)}))

(defn coerce-sql-date
  "Coerce from SQL date if not null"
  [date]
  (when date (coerce/from-sql-date date)))

(k/defentity state
             (k/entity-fields
               :name
               :theDate))

(k/defentity olddoi
  (k/pk :doi)
  (k/entity-fields
    :doi
    :issuedDate
    :issuedString
    :redepositedDate
    :firstDepositedDate
    :resolved
    :firstResolution
    :ultimateResolution
    :firstResolutionLog)
  
  (k/transform
    (fn [input]
      (when input
        (assoc input
          :issuedDate (coerce-sql-date (:issuedDate input))
          :redepositedDate (coerce-sql-date (:redepositedDate input))
          :firstDepositedDate (coerce-sql-date (:firstDepositedDate input))
          :resolved (coerce-sql-date (:resolved input)))))))

(k/defentity doi
  (k/pk :doi)
  (k/entity-fields :doi :id))

(k/defentity sources
  (k/pk :id)
  (k/entity-fields :id :ident :name))

(k/defentity types
  (k/pk :id)
  (k/entity-fields :id :ident :name :milestone :arg1desc :arg2desc :arg3desc))

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

(defn coerce-timeline-in [timeline]
  "Coerce a timeline to Java Date from Joda Date, so it can be serialized"
  (reduce-kv (fn [m k v] (assoc m (coerce/to-date k) v)) {} timeline))

(defn coerce-timeline-out [timeline]
  (reduce-kv (fn [m k v] (assoc m (coerce/from-date k) v)) {} timeline))

(k/defentity event-timelines
  (k/table "event_timelines")
  (k/pk :id)
  (k/entity-fields
    :id
    :doi
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


(k/defentity referrer-domain-events
  (k/table "referrer_domain_events")
  (k/entity-fields
    :event
    :count
    :host
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
    :host
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
