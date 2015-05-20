(ns chronograph.handlers
  (:require [chronograph.data :as d]
            [chronograph.db :as db]
            [chronograph.util :as util]
            [chronograph.import.mdapi :as mdapi]
            [chronograph.types :as types])
  (:require [clj-time.core :as t]
            [clj-time.coerce :as coerce])
  (:require [compojure.core :refer [context defroutes GET ANY POST]]
            [compojure.handler :as handler]
            [compojure.route :as route])
  (:require [ring.util.response :refer [redirect]])
  (:require [liberator.core :refer [defresource resource]]
            [liberator.representation :refer [ring-response]])
  (:require [selmer.parser :refer [render-file cache-off!]]
            [selmer.filters :refer [add-filter!]])
  (:require [clojure.data.json :as json])
  (:require [crossref.util.doi :as crdoi]
            [crossref.util.config :refer [config]])
  (:require [clj-time.format :as f])
  (:require [clojure.walk :refer [prewalk]])
  (:require [clojure.core.async :as async :refer [<! <!! >!! >! go chan]])
  (:require [org.httpkit.server :refer [with-channel on-close on-receive send! run-server]]))

(add-filter! :name name)
(add-filter! :is-url #(or (.startsWith % "http://") (.startsWith % "https://")))

; This can run as "Chronograph" or "DOI Event Collection"
(def site-title (or (:title config) "DOI Chronograph"))
(def homepage-template (or (:homepage-template config) "templates/index.html"))

(def iso-format (f/formatters :date-time))

(defn convert-all-dates
  "Convert all dates in structure"
  [input fmt]
    (prewalk #(if (= (type %) org.joda.time.DateTime)
              (condp = fmt
                :iso8601 (f/unparse iso-format %)
                :seconds (/ (coerce/to-long %) 1000)
                ; Otherwise return it unchanged
                %)%)
           input))

(defn export-info
  "Export with string keys (suitable for various content types)"
  [info]
  (when info 
      {
        "doi" (:doi info)
        "firstResolutionLog" (:firstResolutionLog info)
        "firstResolution" (:firstResolution info)
        "ultimateResolution" (:ultimateResolution info)
        "issuedDate" (str (:issuedDate info))
        "issuedString" (:issuedString info)
        "redepositedDate" (str (:redepositedDate info))
        "firstDepositedDate" (str (:firstDepositedDate info))
        "resolved" (str (:resolved info))}))


; A collection of async channels via which push events will be rebroadcast.
(def listeners (atom {}))
(defn register-listener [listener-chan event-type]
  (swap! listeners assoc listener-chan event-type))

(defn unregister-listener [listener-chan]
  (swap! listeners dissoc listener-chan))

(def recent-broadcast-events-size 200)
(def recent-broadcast-events (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn enqueue-broadcast [item]
  (swap! recent-broadcast-events (fn [old-queue]
                                   (let [new-queue (conj old-queue item)]
                                     (if (> (.size new-queue) recent-broadcast-events-size)
                                       (pop new-queue)
                                       new-queue)))))

(defresource dois
  []
  :allowed-methods [:post]
  :available-media-types ["text/html" "application/json" "text/csv"]
  :post-redirect? false
  :new? false
  :exists? true
  :respond-with-entity? true
  :multiple-resolutions? false
  :handle-ok (fn [ctx]
                ; So many different ways of getting the upload!
                (let [dois-input-body (when-let [body (-> ctx :request :body)] (slurp body))
                      dois-input-file (when-let [body (-> ctx :request :params :upload :tempfile)] (slurp body))
                      dois-input-upload (when-let [body (-> ctx :request :params :upload)] body)
                      dois-input (or 
                                   (and (not= "" dois-input-body) dois-input-body)
                                   (and (not= "" dois-input-file) dois-input-file)
                                   (and (not= "" dois-input-upload) dois-input-upload))
                      dois (.split dois-input "\r?\n" )
                      results []
                      ; TODO removed until DOI denormalization resolved
                      ; results (map (fn [doi]
                      ;                   (when doi (export-info (d/get-doi-info doi)))) dois)
                      results (remove nil? results)    
                      results (if (empty? results) nil results)]
                  results)))

; Fetch tokens on start-up. They're almost never changed.
(def tokens (d/get-tokens))

(defresource push
  []
  :allowed-methods [:post]
  :available-media-types ["application/json"]
  :post-redirect? false
  :malformed? (fn [ctx]
                (let [token (get-in ctx [:request :headers "token"])
                     body (slurp (get-in ctx [:request :body]))]
                (try 
                  (let [body-content (json/read-str body)
                        doi (get body-content "doi")
                        type-name (get types/type-names (keyword (get body-content "type")))
                        ; ensure it's a recognised type name
                        source-name (get types/source-names (keyword (get body-content "source")))
                        
                        storage-type ((get types/types-by-name type-name) :storage)
                        storage-type-allowed (#{:event :milestone :fact} storage-type)
                        
                        arg1 (get body-content "arg1")
                        arg2 (get body-content "arg2")
                        arg3 (get body-content "arg3")]
                    
                    ; DOI not required, it may be a heartbeat.
                    (if (or (empty? token) (nil? type-name) (nil? source-name) (not storage-type-allowed))
                      true
                      [false {::token token
                              ::event {:doi doi
                                       :type-name type-name
                                       :source-name source-name
                                       :storage-type storage-type
                                       :arg1 arg1
                                       :arg2 arg2
                                       :arg3 arg3}}]))
                  ; JSON deserialization errors.
                  (catch java.io.EOFException _ true)
                  (catch java.lang.Exception _ true))))
  
  :authorized? (fn [ctx]
                (let [event (::event ctx)
                      token (::token ctx)
                      type-name (:type-name event)
                      source-name (:source-name event)
                      got-token (get tokens token)
                      type-allowed (get-in got-token [:allowed-types type-name])
                      source-allowed (get-in got-token [:allowed-sources source-name])]
                  ; Must be allowed to insert events both of the the type and source.
                  (and type-allowed source-allowed)))
  :new? false
  :exists? true
  :respond-with-entity? true
  :multiple-resolutions? false

  :handle-ok (fn [ctx]
               (let [event (::event ctx)
                     doi (:doi event)
                     type-name (:type-name event)
                     source-name (:source-name event)
                     arg1 (:arg1 event)
                     arg2 (:arg2 event)
                     arg3 (:arg3 event)
                     ; If there's no DOI, record this as a heartbeat.
                     is-heartbeat (or (nil? doi) (empty? doi))
                     type-format-function (:format (types/types-by-name type-name) identity)
                     ]
                 
                 ; Can't insert :timeline with API
                (when-not is-heartbeat
                  (let [doi (crdoi/non-url-doi doi)
                        now (t/now)
                        
                        broadcast-event (json/write-str {:storage-type (::storage-type ctx)
                                                        :type type-name
                                                        :doi doi
                                                        :source-name source-name
                                                        :date (f/unparse iso-format now)
                                                        :arg1 arg1
                                                        :arg2 arg2
                                                        :arg3 arg3
                                                        :formatted (type-format-function event)})]
                     ; Insert into DB.
                     (condp = (:storage-type event)
                       :event (d/insert-event-async doi type-name source-name now 1 arg1 arg2 arg3)
                       :milestone (d/insert-milestone-async doi type-name source-name now 1 arg1 arg2 arg3)
                       :fact (d/insert-fact-async doi type-name source-name now 1 arg1 arg2 arg3))
                     
                     ; Broadcast to listeners who are interested in this type.
                     (doseq [[channel channel-type-name] @listeners]
                       (when (= type-name channel-type-name)
                        (send! channel broadcast-event)))
                     
                     ; Also stick on the queue so new joiners can see recent history.
                     (enqueue-broadcast [type-name broadcast-event])))
                
                (if is-heartbeat
                 (d/inc-heartbeat-bucket type-name)
                 (d/inc-push-bucket type-name))
               "OK")))

(defresource doi-facts
  [doi-prefix doi-suffix]
  :allowed-methods [:get]
  :available-media-types ["text/html" "application/json" "text/csv"]
  :exists? (fn [ctx]
            (let [doi (str doi-prefix "/" doi-suffix)
                  info (d/get-doi-facts doi)]
              [info {::info info}]))
  :handle-ok (fn [ctx]
              (export-info (::info ctx))
              (::info ctx)))

(defresource doi-events
  [doi-prefix doi-suffix]
  :allowed-methods [:get]
  :available-media-types ["text/html" "application/json" "text/csv"]
  :exists? (fn [ctx]
            (let [doi (str doi-prefix "/" doi-suffix)
                  info (d/get-doi-events doi)]
              [info {::info info}]))
  :handle-ok (fn [ctx]
              (export-info (::info ctx))
              (::info ctx)))
 
(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               ; Homepage template can be specified by config.
   (render-file homepage-template {:site-title site-title})))

(defresource member-domains
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
   (render-file "templates/member-domains.html" {:site-title site-title 
                                                 :member-domains (sort d/member-domains)})))

(defresource top-domains
  []
  :available-media-types ["text/html"]
  :malformed? (fn [ctx]
                      (let [top (try
                                  (. Integer parseInt (or (-> ctx :request :params :top) "200"))
                                  (catch java.lang.NumberFormatException _ nil))]
                        [(not top) {::top top}]))
  :handle-ok (fn [ctx]
               (let [; Include special things like 'no-referrer'
                     include-special (= (-> ctx :request :params :special) "true")
                     results (d/get-top-domains-ever true false (::top ctx) include-special)]
                 (render-file "templates/top-domains.html" {:site-title site-title
                                                            :include-members false
                                                            :top-domains results
                                                            :top (::top ctx)}))))

(defresource top-domains-members
  []
  :available-media-types ["text/html"]
  :malformed? (fn [ctx]
                      (let [
                            top (try
                                  (. Integer parseInt (or (-> ctx :request :params :top) "200"))
                                  (catch java.lang.NumberFormatException _ nil))]
                        [(not top) {::top top}]))
  :handle-ok (fn [ctx]
               (let [; Include special things like 'no-referrer'
                     include-special (= (-> ctx :request :params :special) "true")
                     results (d/get-top-domains-ever true true (::top ctx) include-special)]
                 (render-file "templates/top-domains.html" {:site-title site-title
                                                            :include-members true
                                                            :top-domains results
                                                            :top (::top ctx)}))))

(defresource dois-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [doi (-> ctx :request :params :doi)]
            [doi {::doi doi}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/dois/" (::doi ctx))))))

(defresource doi-page
  [doi]
  :available-media-types ["text/html" "application/json"]
  :handle-ok (fn [ctx]
               (let [events (d/get-doi-events doi)
                     facts (d/get-doi-facts doi)
                     milestones (d/get-doi-milestones doi)
                     timelines (d/get-doi-timelines doi)
                     
                     timeline-dates (apply merge (map #(keys (:timeline %)) timelines))
                     continuous-events (remove :milestone events)
                     milestone-events (filter :milestone events)
                      
                     ; Merge dates from events with dates from timelines
                     all-dates (concat timeline-dates (map :event events) (map :event milestones))
                     all-dates-sorted (sort t/before? all-dates)
                     first-date (when all-dates-sorted (first all-dates-sorted))
                     last-date (when all-dates-sorted (last all-dates-sorted))
                     interpolated-timelines (when (and first-date last-date) (map #(assoc % :timeline (d/interpolate-timeline (:timeline %) first-date last-date (t/days 1))) timelines))

                      date-format (condp = (get-in ctx [:representation :media-type])
                                   "text/html" :seconds
                                   "application/json" :iso8601)
                     
                    
                     
                     response {:first-date first-date
                               :last-date last-date
                               :doi doi
                               :events (map types/export-type-info events)
                               :milestones (map types/export-type-info milestones)
                               :facts (map types/export-type-info facts)
                               :timelines (map types/export-type-info interpolated-timelines)
                               }
                     
                     ; Dates in response need converting. This is sent back as JSON.
                     ; Dates in render-context can be 'real' dates as they're consumed by the template.
                     response-all-dates-converted (convert-all-dates response date-format)
                     
                     ; The dates in the timeline are used in JavaScript plotting, so convert only those for the HTML.
                     ; Don't touch others as they need to be real dates for the template rendering.
                     response-timeline-dates-converted (assoc response :timelines (convert-all-dates (:timelines response) date-format))
                     
                     render-context {:site-title site-title
                                     :first-date first-date
                                     :last-date last-date
                                     :response response-timeline-dates-converted}]
                 
                  (condp = (get-in ctx [:representation :media-type])
                    
                    "text/html" (render-file "templates/doi.html" render-context)
                    "application/json" (json/write-str response-all-dates-converted)))))

(defresource domains-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [domain (-> ctx :request :params :domain)]
            [domain {::domain domain}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/domains/" (::domain ctx))))))

(defresource domain-page
  [host]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [[subdomain true-domain etld] (util/get-main-domain host)
                     domain (.toLowerCase (str true-domain "." etld))
                     
                     whitelisted (d/domain-whitelisted? domain)
                     
                     events (d/get-domain-events domain)
                     facts (d/get-domain-facts domain)
                     timelines (d/get-domain-timelines domain)
                     timeline-dates (apply merge (map #(keys (:timeline %)) timelines))
                                       
                     ; events-by-type (group-by :type-name events)
                     
                     all-dates (concat timeline-dates (map :event events))
                     all-dates-sorted (sort t/before? all-dates)
                     first-date (first all-dates-sorted)
                     last-date (last all-dates-sorted)
                     
                     interpolated-timelines (when (and first-date last-date) (map #(assoc % :timeline (d/interpolate-timeline (:timeline %) first-date last-date (t/days 1))) timelines))
                     
                     date-format (condp = (get-in ctx [:representation :media-type])
                     "text/html" :seconds
                     "application/json" :iso8601)

                     timelines-dates-converted (convert-all-dates interpolated-timelines date-format)

                     subdomains (reverse (sort-by :count (d/get-subdomains-for-domain true-domain true)))
                     
                     render-context {:site-title site-title
                                     :first-date first-date
                                     :last-date last-date                                     
                                     :domain domain
                                     :events events
                                     :facts facts
                                     :timelines timelines-dates-converted
                                     :subdomains subdomains
                                     :whitelisted whitelisted}]
               (render-file "templates/domain.html" render-context))))


(defresource doi-domain-page
  [doi host]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (let [[subdomain true-domain etld] (util/get-main-domain host)
                     domain (.toLowerCase (str true-domain "." etld))
                     
                     whitelisted (d/domain-whitelisted? domain)
                     
                     timelines (d/get-doi-domain-timelines doi host)
                     timeline-dates (apply merge (map #(keys (:timeline %)) timelines))
                                       
                     all-dates-sorted (sort t/before? timeline-dates)
                     first-date (first all-dates-sorted)
                     last-date (last all-dates-sorted)
                     
                     interpolated-timelines (when (and first-date last-date) (map #(assoc % :timeline (d/interpolate-timeline (:timeline %) first-date last-date (t/months 1))) timelines))
                     date-format (condp = (get-in ctx [:representation :media-type])
                                   "text/html" :seconds
                                   "application/json" :iso8601)
                     
                     timelines-dates-converted (convert-all-dates interpolated-timelines date-format)
                     
                     render-context {:site-title site-title
                                     :first-date first-date
                                     :last-date last-date
                                     :domain domain
                                     :doi doi
                                     :timelines timelines-dates-converted
                                     :whitelisted whitelisted}]
                                
               (render-file "templates/doi-domain.html" render-context))))


; Get list of DOIs for DOI-Domain timelines with this domain.
(defresource domain-dois-page
  [host]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [page-size 50
                     offset (try (Integer/parseInt (-> ctx :request :params :offset)) (catch java.lang.NumberFormatException _ 0))
                     limit (+ offset page-size)

                     [subdomain true-domain etld] (util/get-main-domain host)
                     domain (.toLowerCase (str true-domain "." etld))
                     
                     whitelisted (d/domain-whitelisted? domain)
                     
                     dois (d/get-available-doi-domain-timelines-for-domain host offset limit)
                     
                     prev-offset (when (> offset 0) (max 0 (- offset page-size)))
                     next-offset (when (> (count dois) 0) (+ offset page-size))
                     
                     render-context {:site-title site-title
                                     :host host
                                     :whitelisted whitelisted
                                     :dois dois
                                     :next-offset next-offset
                                     :prev-offset prev-offset}]
                 (render-file "templates/domain-dois-domain-list.html" render-context))))

; Get list of domains for DOI-Domain timelines with this DOI.
(defresource doi-domains-page
  [doi]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [page-size 50
                     offset (try (Integer/parseInt (-> ctx :request :params :offset)) (catch java.lang.NumberFormatException _ 0))
                     limit (+ offset page-size)

                     domains (d/get-available-doi-domain-timelines-for-doi doi offset limit)
                     whitelisted (filter d/domain-whitelisted? domains)

                     prev-offset (when (> offset 0) (max 0 (- offset page-size)))
                     next-offset (when (> (count domains) 0) (+ offset page-size))
                     
                     render-context {:site-title site-title
                                     :doi doi
                                     :whitelisted whitelisted
                                     :hosts whitelisted
                                     :next-offset next-offset
                                     :prev-offset prev-offset
                                     }]
                 (render-file "templates/domain-dois-doi-list.html" render-context))))



(defresource subdomains-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [subdomain (-> ctx :request :params :subdomain)]
            [subdomain {::subdomain subdomain}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/subdomains/" (::subdomain ctx))))))

(defresource subdomain-page
  [host]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [[subdomain true-domain etld] (util/get-main-domain host)
                     domain (.toLowerCase (str true-domain "." etld))
                     
                     whitelisted (d/domain-whitelisted? domain)
                     
                     events (d/get-subdomain-events host)
                     facts (d/get-subdomain-facts host)
                     timelines (d/get-subdomain-timelines host)
                     timeline-dates (apply merge (map #(keys (:timeline %)) timelines))
                                                            
                     all-dates (concat timeline-dates (map :event events))
                     all-dates-sorted (sort t/before? all-dates)
                     first-date (first all-dates-sorted)
                     last-date (last all-dates-sorted)
                     
                     interpolated-timelines (when (and first-date last-date) (map #(assoc % :timeline (d/interpolate-timeline (:timeline %) first-date last-date (t/days 1))) timelines))
                     
             
                    date-format (condp = (get-in ctx [:representation :media-type])
                      "text/html" :seconds
                      "application/json" :iso8601)

                    timelines-dates-converted (convert-all-dates interpolated-timelines date-format)
                     
                     subdomains (reverse (sort-by :count (d/get-subdomains-for-domain true-domain true)))
                     
                     render-context {:site-title site-title
                                     :first-date first-date
                                     :last-date last-date
                                     :subdomain host
                                     :domain domain
                                     :events events
                                     :facts facts
                                     :timelines timelines-dates-converted
                                     :subdomains subdomains
                                     :whitelisted whitelisted}]
                 
               (render-file "templates/subdomain.html" render-context))))

(defresource event-types
  [type-name]
  :available-media-types ["text/html"]
  :exists? (fn [ctx]
                (let [page-size 50
                      offset (try (Integer/parseInt (-> ctx :request :params :offset)) (catch java.lang.NumberFormatException _ 0))
                      limit (+ offset page-size)
                      type-name (keyword type-name)
                      type (get types/types-by-name type-name)
                      storage (:storage type)
                      ; Not meaningful to show for timelines (which don't really have a concrete 'time') or facts which manifestly don't.
                      events (condp = storage
                              :timeline nil
                              :event (d/get-recent-events (:name type) offset limit)
                              :milestone (d/get-recent-milestones (:name type) offset limit)
                              :fact nil
                              nil)
                      
                      prev-offset (when (> offset 0) (max 0 (- offset page-size)))
                      next-offset (when (> (count events) 0) (+ offset page-size))]
                  
                  [(and type events) {::type type
                                      ::events events
                                      ::storage storage
                                      ::prev-offset prev-offset
                                      ::next-offset next-offset}]))
  
  :handle-ok (fn [ctx]
               (let [type (::type ctx)
                     storage (::storage ctx)
                     events (::events ctx)
                     num-events (::num-events ctx)
                     with-info (map types/export-type-info events)]
                 (render-file "templates/events.html" {:site-title site-title
                                                       :events with-info
                                                       :type type
                                                       :next-offset (::next-offset ctx)
                                                       :prev-offset (::prev-offset ctx)}))))

; Just serve up a blank page with JavaScript to pick up from event-types-socket.
(defresource event-types-live
  [type-name]
  :available-media-types ["text/html"]
  :exists? (fn [ctx]
                (let [type-name (keyword type-name)
                      type (get types/types-by-name type-name)
                      storage (:storage type)]
                  
                  [true {::type type
                         ::storage storage}]))
  
  :handle-ok (fn [ctx]
               (let [type (::type ctx)
                     storage (::storage ctx)
                     events (::events ctx)
                     num-events (::num-events ctx)
                     with-info (map types/export-type-info events)]
                 (render-file "templates/events-live.html" {:site-title site-title
                                                       :type type
                                                       :next-offset (::next-offset ctx)
                                                       :prev-offset (::prev-offset ctx)}))))

(defn event-types-socket
  [request]
  (let [type-name (-> request :params :type-name)
        typ (keyword type-name)
        ]
   (with-channel request output-channel
                 
    ; For starters, catch up with recent history.
    (doseq [[event-type event] @recent-broadcast-events]
      (when (= event-type typ)
        (send! output-channel event)))
    
    (register-listener output-channel typ)
    
    (on-close output-channel
              (fn [status]
                (unregister-listener output-channel)))
    (on-receive output-channel
              (fn [data])))))

(defresource status
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [
                     types-with-count (map #(assoc % :count (d/type-table-count (:name %))
                                                     :storage-description (types/storage-descriptions (:storage %))
                                                     :show-events-link (#{:event :milestone} (:storage %))
                                                     :recent-heartbeats (d/get-recent-heartbeats (:name %))) types/types)
                     
                     ; Don't show types with no events.
                     ; Not least becuase this is used for two purposes, no point confusing types.
                     types-with-count (filter #(> (:count %) 0) types-with-count)]
               (render-file "templates/status.html" {:site-title site-title
                                                     :types types-with-count}))))



(defroutes app-routes
  
  (GET "/" [] (home))
  (GET "/status" [] (status))
  (GET "/member-domains" [] (member-domains))
  (GET "/top-domains-members" [] (top-domains-members))
  (GET "/top-domains" [] (top-domains))
  (GET ["/events/types/:type-name" :type-name #"[^/]*"] [type-name] (event-types type-name))
  (GET ["/events/types/:type-name/live" :type-name #"[^/]*"] [type-name] (event-types-live type-name))
  (GET ["/events/types/:type-name/live/socket" :type-name #"[^/]*"] [type-name] event-types-socket)
  
  
  (context "/dois" []
    (GET "/" [] (dois-redirect))
    ; Nesting contexts doesn't work with this type of parameter capture.
    (ANY "/*/domains" {{doi :*} :params} (doi-domains-page doi))
    (ANY "/*" {{doi :*} :params} (doi-page doi)))
  (context "/domains" []
    (GET "/" [] (domains-redirect))
    (context ["/:domain" :domain #".+?"] [domain]
      (GET "/" [] (domain-page domain))
      (GET "/dois" [] (domain-dois-page domain))))
  (context "/subdomains" []
    (GET "/" [] (subdomains-redirect))
    (context ["/:subdomain" :subdomain #".+?"] [subdomain] (subdomain-page subdomain)))
  
  (ANY "/domain-dois/:domain/*" {{domain :domain doi :*} :params} (doi-domain-page doi domain))

  
  
  (context "/api" []
    (POST "/push" [] (push))
    (context "/dois" []
      (POST "/" [] (dois))
      (context ["/:doi-prefix/:doi-suffix/facts" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-facts doi-prefix doi-suffix))
      (context ["/:doi-prefix/:doi-suffix/events" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-events doi-prefix doi-suffix))))
  
  (route/resources "/")
  )

(def app
  (-> app-routes
      handler/site))
