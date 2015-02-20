(ns chronograph.handlers
  (:require [chronograph.data :as d]
            [chronograph.db :as db]
            [chronograph.util :as util]
            [chronograph.import.mdapi :as mdapi]
            [chronograph.types :as types])
  (:require [clj-time.core :as t])
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
            [crossref.util.config :refer [config]]))

(add-filter! :name name)

; This can run as "Chronograph" or "DOI Event Collection"
(def title (or (:title config) "DOI Chronograph"))
(def homepage-template (or (:homepage-template config) "templates/index.html"))

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
                        arg1 (get body-content "arg1")
                        arg2 (get body-content "arg2")
                        arg3 (get body-content "arg3")]
                    (if (or (empty? doi) (empty? token) (nil? type-name) (nil? source-name))
                      true
                      [false {::doi (crdoi/non-url-doi doi) 
                              ::token token
                              ::type-name type-name
                              ::source-name source-name
                              ::arg1 arg1
                              ::arg2 arg2
                              ::arg3 arg3}]))
                  ; JSON deserialization errors.
                  (catch java.io.EOFException _ true)
                  (catch java.lang.Exception _ true))))
  
  :authorized? (fn [ctx]
                (let [token (::token ctx)
                      type-name (::type-name ctx)
                      source-name (::source-name ctx)
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
               (let [doi (::doi ctx)
                     type-name (::type-name ctx)
                     source-name (::source-name ctx)
                     arg1 (::arg1 ctx)
                     arg2 (::arg2 ctx)
                     arg3 (::arg3 ctx)]
                 ; TODO DECIDE ON TYPE
               (d/insert-event-async doi type-name source-name (t/now) 1 arg1 arg2 arg3))
               "OK"))

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

; A quick list manually hacked
; select distinct(events.doi), length(event_timelines.timeline) as e  from event_timelines inner join events on events.doi = event_timelines.doi where events.event > "2013-01-01" order by e desc limit 50;
(def interesting-dois [
  "10.1787/20752288-table-tur"
  "10.1038/cddis.2009.22"
  "10.1038/471305c"
  "10.1787/20752288-table-fra"
  "10.1016/0304-405X"
  "10.1038/ni.1863"
  "10.3322/caac.20006"
  "10.1093/ndt"
  "10.1007/b96702"
  "10.1016/j.nuclphysa.2003.11.001"
  "10.1038/nprot.2007.30"
  "10.1787/20758510-table6"
  "10.1038/nbt1037"
  "10.1007/s12028-012-9695-z"
  "10.1038/nature05913"
  "10.1385/1-59259-384-4:3"
  "10.1787/20743866-table4"
  "10.1038/nn775"
  "10.1038/nature11400"
  "10.1016/j.annemergmed.2006.03.031"
  "10.1016/j.autrev.2007.08.003"
  "10.1038/21987"
  "10.1038/nature09270"
  "10.1056/NEJMoa0802743"
  "10.1016/j.fluid.2007.07.023"
  "10.1021/je049540s"
  "10.1021/je050316s"
  "10.1038/nprot.2007.418"
  "10.1016/j.addbeh.2009.03.028"
  "10.1016/j.livsci.2011.03.006"
  "10.3201/eid1805.111916"
  "10.1016/S0140-6736"
  "10.1002/hep.25578"
  "10.1007/s10071-010-0371-4"
  "10.1007/s13253-010-0033-7"
  "10.1016/j.cell.2007.06.014"
  "10.1371/journal.pone.0038724"
  "10.3382/ps.2009-00360"
  "10.1080/09603100410001673676"
  "10.1007/978-90-481-8622-8_8"          
])
 
(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               ; Homepage template can be specified by config.
   (render-file homepage-template {:title title
                                        :interesting-dois interesting-dois})))

(defresource member-domains
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
   (render-file "templates/member-domains.html" {:title title 
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
                 (render-file "templates/top-domains.html" {:title title
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
                 (render-file "templates/top-domains.html" {:title title
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
  :available-media-types ["text/html"]
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
                     timelines-with-extras (map #(assoc % :min (when (not-empty (:timeline %)) (apply min (map second (:timeline %))))
                                                          :max (when (not-empty (:timeline %)) (apply max (map second (:timeline %)))))
                                                 interpolated-timelines)

                     ;; add 1 day of padding either side for charting
                     first-date-pad (when first-date (t/minus first-date (t/days 1)))
                     last-date-pad (when last-date (t/plus last-date (t/days 1)))
                     
                     ; get extra info in
                     extra-info nil; (mdapi/get-metadata doi)
                     
                     render-context {:title title
                                     :first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :doi doi
                                     :events events
                                     :milestones milestones
                                     :facts facts
                                     :timelines timelines-with-extras
                                     :extra-info extra-info}]                 
               (render-file "templates/doi.html" render-context))))

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
                     timelines-with-extras (map #(assoc % :min (when (not-empty (:timeline %)) (apply min (map second (:timeline %))))
                                                          :max (when (not-empty (:timeline %)) (apply max (map second (:timeline %)))))
                                                 interpolated-timelines)
                    
                     ;; add 1 day of padding either side for charting
                     first-date-pad (when first-date (t/minus first-date (t/days 1)))
                     last-date-pad (when last-date (t/plus last-date (t/days 1)))
                     
                     subdomains (reverse (sort-by :count (d/get-subdomains-for-domain true-domain true)))
                     
                     render-context {:title title
                                     :first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :domain domain
                                     :events events
                                     :facts facts
                                     :timelines timelines-with-extras
                                     :subdomains subdomains
                                     :whitelisted whitelisted
                                     }]
               (render-file "templates/domain.html" render-context))))

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
                     timelines-with-extras (map #(assoc % :min (when (not-empty (:timeline %)) (apply min (map second (:timeline %))))
                                                          :max (when (not-empty (:timeline %)) (apply max (map second (:timeline %)))))
                                                 interpolated-timelines)
                    
                     ;; add 1 day of padding either side for charting
                     first-date-pad (when first-date (t/minus first-date (t/days 1)))
                     last-date-pad (when last-date (t/plus last-date (t/days 1)))
                     
                     subdomains (reverse (sort-by :count (d/get-subdomains-for-domain true-domain true)))
                     
                     render-context {:title title
                                     :first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :subdomain host
                                     :domain domain
                                     :events events
                                     :facts facts
                                     :timelines timelines-with-extras
                                     :subdomains subdomains
                                     :whitelisted whitelisted}]
               (render-file "templates/subdomain.html" render-context))))

(defresource event-types
  [type-name]
  :available-media-types ["text/html"]
  :exists? (fn [ctx]
                (let [type-name (keyword type-name)
                      type (get types/types-by-name type-name)]
                  [type {::type type}]))
  
  :handle-ok (fn [ctx]
               (let [num-events 50
                     type (::type ctx)
                     events (d/get-recent-events (:name type) num-events)]
                 (render-file "templates/events.html" {:title title
                                                       :events events
                                                       :type type
                                                       :num-events num-events}))))



(defroutes app-routes
  
  (GET "/" [] (home))
  (GET "/member-domains" [] (member-domains))
  (GET "/top-domains-members" [] (top-domains-members))
  (GET "/top-domains" [] (top-domains))
  (GET ["/events/types/:type-name" :type-name #".*"] [type-name] (event-types type-name))
  (context "/dois" []
    (GET "/" [] (dois-redirect))
    (ANY "/*" {{doi :*} :params} (doi-page doi)))
  (context "/domains" []
    (GET "/" [] (domains-redirect))
    (context ["/:domain" :domain #".+?"] [domain] (domain-page domain)))
  (context "/subdomains" []
    (GET "/" [] (subdomains-redirect))
    (context ["/:subdomain" :subdomain #".+?"] [subdomain] (subdomain-page subdomain)))
  
  (context "/api" []
    (POST "/push" [] (push))
    (context "/dois" []
      (POST "/" [] (dois))
      (context ["/:doi-prefix/:doi-suffix/facts" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-facts doi-prefix doi-suffix))
      (context ["/:doi-prefix/:doi-suffix/events" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-events doi-prefix doi-suffix))))
  
  (route/resources "/"))

(def app
  (-> app-routes
      handler/site))
