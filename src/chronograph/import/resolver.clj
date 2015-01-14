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
  (:require [clojure.core.async :as async :refer [<! <!! >!! >! go chan close!]]))

(def doi-channel (chan))

(def issued-type-id (data/get-type-id-by-name "issued"))
(def resolved-type-id (data/get-type-id-by-name "first-resolution-test"))
(def source-id (data/get-source-id-by-name "CrossRefRobot"))

(defn prnl [& args] (locking *out* (apply prn args)))

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [doi]
  (try 
    (locking *out* (prn "Start resolve" doi))
    (let [url (crdoi/normalise-doi doi)
          result (try-try-again {:sleep 5000 :tries 2} #(client/head url
                                                         {:follow-redirects true
                                                          :throw-exceptions false
                                                          :socket-timeout 5000
                                                          :conn-timeout 5000
                                                          :headers {"Referer" "chronograph.crossref.org"}}))
          redirects (:trace-redirects result)
          first-redirect (second redirects)
          last-redirect (last redirects)
          ok (= 200 (:status result))]
          (locking *out* (prn "Finish resolve" doi ok))
          (when ok
            [first-redirect last-redirect]))
    (catch Exception _ nil)))

(defn insert-resolutions
  [doi first-direct last-redirect]
  (locking *out* (prn "Insert resolution" doi))
    (let [resolutions (get-resolutions doi)]
        (data/insert-event doi resolved-type-id source-id (t/now) 1 (first resolutions) (second resolutions) nil)
        (k/update d/resolutions (k/where {:doi doi}) (k/set-fields {:resolved true}))))


; Create a load of return channels for resolved DOIs.
(def num-return-chans 50)

(defn run-doi-resolution []
  ; Insert recently published.
  (let [yesterday (t/minus (t/now) (t/days 1))
        recently-published (k/select d/events-isam (k/where {:type issued-type-id :event [>= (coerce/to-sql-date yesterday)]}))]

    (doseq [event recently-published]
      (prn "Add to resolutions table" (:doi event))
      (k/exec-raw ["INSERT INTO resolutions (doi) VALUES (?) ON DUPLICATE KEY UPDATE doi = doi"
                 [(:doi event)]]))
    
    (let [to-resolve (k/select d/resolutions (k/where {:resolved false}))
          dois (map :doi to-resolve)
          doi-resolve-channels (apply vector (map (fn [i] (chan)) (range num-return-chans)))
          resolve-channel (async/merge doi-resolve-channels)]
      
      ; Run concurrent resolvers that poll from the doi-channel and send back results back on their own channels.
      (doseq [ch doi-resolve-channels]
        (go
           (loop [doi (<! doi-channel)]
             (when doi
                   (let [result (get-resolutions doi)]
                     (when result
                       (let [[first-redirect last-redirect] result]
                           (>! ch [doi first-redirect last-redirect]))))
                   (recur (<! doi-channel))))
             
           ; When loop is over, close output channel.
           (prnl "Close channel" ch)
           (close! ch)))
            
      ; Stick the DOIs on a channel for them to be resolved asynchronously and returned via doi-resolve-channels.
      (go
        (doseq [doi dois]
          (locking *out* (prn "Spool" doi))
          (>! doi-channel doi))
        (prn "Closing DOI channel")
        (close! doi-channel))
            
      ; Get resolutions from the merged doi-resolve-channel and insert them.
      ; TODO maybe batch into transactions?
      (loop [resolution (<!! resolve-channel)]
        (when resolution
          (let [[doi first-redirect last-redirect] resolution]
            (insert-resolutions doi first-redirect last-redirect))
          (recur (<!! resolve-channel)))))))
