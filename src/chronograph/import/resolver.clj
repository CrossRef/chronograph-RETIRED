(ns chronograph.import.resolver
  (:require [chronograph.db :as d]
            [chronograph.core :as core]
            [chronograph.util :as util]
            [chronograph.data :as data])
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

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [the-doi]
  (try 
    (let [url (crdoi/normalise-doi the-doi)
          result (try-try-again {:sleep 5000 :tries 2} #(client/get url
                                                         {:follow-redirects true
                                                          :throw-exceptions false
                                                          :headers {"Referer" "chronograph.crossref.org"}}))
          redirects (:trace-redirects result)
          first-redirect (second redirects)
          last-redirect (last redirects)
          ok (= 200 (:status result))]
          (when ok
            [first-redirect last-redirect]))
    (catch Exception _ nil)))

(defn run-doi-resolution []
  ; Insert recently published.
  (let [issued-type-id (data/get-type-id-by-name "issued")
        resolved-type-id (data/get-type-id-by-name "first-resolution-test")
        source-id (data/get-source-id-by-name "CrossRefRobot")
        yesterday (t/minus (t/now) (t/weeks 1))
        recently-published (k/select d/events-isam (k/where {:type issued-type-id :event [>= (coerce/to-sql-date yesterday)]}))]
    (doseq [event recently-published]
      (prn "Add to resolutions table" (:doi event))
      (k/exec-raw ["INSERT INTO resolutions (doi) VALUES (?) ON DUPLICATE KEY UPDATE doi = doi"
                 [(:doi event)]]))
    
    (let [to-resolve (k/select d/resolutions (k/where {:resolved false}))
          dois (map :doi to-resolve)]
      (doseq [doi dois]
        ; Try to resolve. It it works, add the event.
        (let [resolutions (get-resolutions doi)]
          (when resolutions
            (prn "Insert" doi)
            (data/insert-event doi resolved-type-id source-id (t/now) 1 (first resolutions) (second resolutions) nil)
            (k/update d/resolutions (k/where {:doi doi}) (k/set-fields {:resolved true}))))))))
