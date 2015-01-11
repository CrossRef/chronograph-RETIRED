(ns chronograph.handlers
  (:require [chronograph.data :as d]
            [chronograph.db :as db]
            [chronograph.util :as util])
  (:require [clj-time.core :as t])
  (:require [compojure.core :refer [context defroutes GET ANY POST]]
            [compojure.handler :as handler]
            [compojure.route :as route])
  (:require [ring.util.response :refer [redirect]])
  (:require [liberator.core :refer [defresource resource]]
            [liberator.representation :refer [ring-response]])
  (:require [selmer.parser :refer [render-file cache-off!]]))

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
"10.1007/s12098-012-0960-0"     
"10.1007/s12098-012-0869-7"     
"10.1007/s12098-012-0945-z"     
"10.1111/cmi.12155"             
"10.1111/cmi.12154"             
"10.1007/s12098-012-0924-4"     
"10.1111/ddg.12073"             
"10.1080/00986445.2012.746673"  
"10.1111/bij.12071"             
"10.1007/s12098-013-1031-x"     
"10.3892/or.2013.2512"          
"10.1111/bij.12072"             
"10.1556/jba.2.2013.003"        
"10.1111/bij.12066"             
"10.1007/s12098-012-0883-9"     
"10.1103/physrevb.87.241403"    
"10.1007/s12098-013-0968-0"     
"10.1088/0031-8949/88/01/015402"
"10.1111/ddg.12009"             
"10.1111/dmcn.12156"            
"10.1542/peds.2013-1099"        
"10.1016/j.irle.2012.11.004"    
"10.1007/s12098-012-0943-1"     
"10.1556/jba.2.2013.002"        
"10.1080/00927872.2012.661005"  
"10.3892/ijmm.2013.1418"        
"10.1111/cobi.12079"            
"10.1111/cp.12008"              
"10.1111/ddg.12088"             
"10.1542/peds.2013-0943"        
"10.1111/bdi.12084"             
"10.1542/peds.2013-0940"        
"10.1111/cpsp.12032"            
"10.1111/bdi.12083"             
"10.1111/bij.12074"             
"10.1111/bij.12082"             
"10.3892/or.2013.2510"          
"10.1542/peds.2013-1056"        
"10.1080/01402382.2012.749652"  
"10.1111/birt.12041"            
"10.1111/cobi.12085"            
"10.1103/physrevb.87.214510"    
"10.1103/physrevb.87.245110"    
"10.1103/physrevb.87.245418"    
"10.1016/j.irle.2012.10.005"    
"10.1088/0031-8949/88/01/015704"
"10.1080/01402382.2013.783353"  
"10.1111/bdi.12078"             
"10.1007/s12098-013-1078-8"     
"10.1111/cobi.12084"            
])


(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
   (render-file "templates/index.html" {:interesting-dois interesting-dois})))

(defresource redacted-domains
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
   (render-file "templates/redacted-domains.html" {:whitelist (sort d/domain-whitelist)
                                                   :blacklist (sort d/domain-blacklist)
                                                   :unknownlist (sort d/domain-unknownlist)
                                                   })))

(defresource top-domains
  []
  :available-media-types ["text/html"]
  :malformed? (fn [ctx]
                      (let [top (try
                                  (. Integer parseInt (or (-> ctx :request :params :top) "200"))
                                  (catch java.lang.NumberFormatException _ nil))]
                        [(not top) {::top top}]))
  :handle-ok (fn [ctx]
               (let [results (d/get-top-domains-ever true (::top ctx))]
                 (render-file "templates/top-domains.html" {:top-domains results :top (::top ctx)}))))


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
                     timelines (d/get-doi-timelines doi)
                     
                     timeline-dates (apply merge (map #(keys (:timeline %)) timelines))
                     
                     continuous-events (remove :milestone events)
                     milestone-events (filter :milestone events)
                                          
                     facts (d/get-doi-facts doi)
                                          
                     ; Merge dates from events with dates from timelines
                     all-dates (concat timeline-dates (map :event events))
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
                     
                     render-context {:first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :doi doi
                                     :events events
                                     :milestone-events milestone-events
                                     ; :continuous-events continuous-events
                                     ; :continuous-events-by-type continuous-events-by-type
                                     ; :milestone-events-by-type milestone-events-by-type
                                     :facts facts
                                     ; :facts-by-type facts-by-type
                                     :timelines timelines-with-extras
                                     }]
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
                     domain (str true-domain "." etld)
                     
                     whitelisted (d/domain-whitelisted? true-domain)
                     
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
                     
                     subdomains (d/get-subdomains-for-domain true-domain)
                     
                     render-context {:first-date first-date
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
                     domain (str true-domain "." etld)
                     
                     whitelisted (d/domain-whitelisted? true-domain)
                     
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
                     
                     subdomains (d/get-subdomains-for-domain true-domain)
                     
                     render-context {:first-date first-date
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



(defroutes app-routes
  (GET "/" [] (home))
  (GET "/redacted-domains" [] (redacted-domains))
  (GET "/top-domains" [] (top-domains))
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
    (context "/dois" []
      (POST "/" [] (dois))
      (context ["/:doi-prefix/:doi-suffix/facts" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-facts doi-prefix doi-suffix))
      (context ["/:doi-prefix/:doi-suffix/events" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-events doi-prefix doi-suffix))))
  
  (route/resources "/"))

(def app
  (-> app-routes
      handler/site
      ))
