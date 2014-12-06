(ns doi-time.db
  (:require [crossref.util.config :refer [config]])
  (:require [korma.db :as kdb])
  (:require [korma.core :as k])
  (:require [korma.db :refer [mysql with-db defdb]])
  (:require [clj-time.coerce :as coerce])
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

(k/defentity doi
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