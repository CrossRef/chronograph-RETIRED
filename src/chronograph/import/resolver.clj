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
  (:require [clojure.core.async :as async :refer [<! <!! >!! >! go chan close! merge]]))

(def doi-channel (chan))
(def dois-resolved-channel (chan))

(def issued-type-id (data/get-type-id-by-name "issued"))
(def resolved-type-id (data/get-type-id-by-name "first-resolution-test"))
(def source-id (data/get-source-id-by-name "CrossRefRobot"))

(defn get-resolutions
  "Get the first and last redirects or nil if it doesn't exist."
  [doi]
  (try 
    (let [url (crdoi/normalise-doi doi)
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

(defn insert-resolutions
  [doi first-direct last-redirect]
  (locking *out* (prn "Insert resolution" doi))
    (let [resolutions (get-resolutions doi)]
        (data/insert-event doi resolved-type-id source-id (t/now) 1 (first resolutions) (second resolutions) nil)
        (k/update d/resolutions (k/where {:doi doi}) (k/set-fields {:resolved true}))))


; Create a load of return channels for resolved DOIs.
(def num-return-chans 10)
(def doi-resolve-channels (apply vector (map (fn [i] (chan)) (range num-return-chans))))

; Run concurrent resolvers that poll from the doi-channel and send back results back on their own channels.
(doseq [ch doi-resolve-channels]
  (go
     (loop [doi (<! doi-channel)]
       (when doi
         (locking *out* (prn "Get resolutions for" doi))
         (let [result (get-resolutions doi)]
           (if result
             (let [[first-redirect last-redirect] result]
                 (locking *out* (prn "Return to channel" ch doi))
                 (>!! ch [doi first-redirect last-redirect]))
             (do
               (locking *out* (prn "Close ch" ch))
               (close! ch))))
         (recur (<!! doi-channel))))))

(defn run-doi-resolution []
  ; Insert recently published.
  (let [yesterday (t/minus (t/now) (t/weeks 1))
        recently-published (k/select d/events-isam (k/where {:type issued-type-id :event [>= (coerce/to-sql-date yesterday)]}))]
    (doseq [event recently-published]
      (prn "Add to resolutions table" (:doi event))
      (k/exec-raw ["INSERT INTO resolutions (doi) VALUES (?) ON DUPLICATE KEY UPDATE doi = doi"
                 [(:doi event)]]))
    
    (let [to-resolve (k/select d/resolutions (k/where {:resolved false}))
          dois (map :doi to-resolve)
          resolve-channel (merge doi-resolve-channels)]
      
      ; Stick the DOIs on a channel for them to be resolved asynchronously and returned via doi-resolve-channels.
      ; Bit of a hack - put a non-resolvable DOI on the channel to start the worker off.
      ; Otherwise they never get started and never close their doi-resolve-channels.
      (doseq [_ (range num-return-chans)]
        (>!! doi-channel "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"))
      
      (go
        (doseq [doi dois]
          (>!! doi-channel doi))
        (prn "Closing DOI channel")
        (close! doi-channel))
      
      ; Get resolutions from the merged doi-resolve-channel and insert them.
      ; TODO maybe batch into transactions?
      (loop [resolution (<!! resolve-channel)]
        (when resolution
          (let [[doi first-redirect last-redirect] resolution]
            (insert-resolutions doi first-redirect last-redirect))
          (recur (<!! dois-resolved-channel)))))))