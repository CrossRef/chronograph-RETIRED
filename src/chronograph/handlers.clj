(ns chronograph.handlers
  (:require [chronograph.data :as d]
            [chronograph.db :as db])
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
                      results (map (fn [doi]
                                        (when doi (export-info (d/get-doi-info doi)))) dois)
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

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
   (render-file "templates/index.html" {})))

(defresource dois-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [doi (-> ctx :request :params :doi)]
            [doi {::doi doi}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/dois/" (::doi ctx))))))

(defresource doi-page
  [doi-prefix doi-suffix]
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [doi (str doi-prefix "/" doi-suffix)
                           doi-id (when doi (d/get-doi-id doi false))]
            [doi-id {::doi-id doi-id ::doi doi}]))
  :handle-ok (fn [ctx]
               (let [doi (::doi ctx)
                     doi-id (::doi-id ctx)
                     events (d/get-doi-events doi)
                     
                     continuous-events (remove :milestone events)
                     milestone-events (filter :milestone events)
                     
                     continuous-events-by-type (group-by :type-name continuous-events)
                     milestone-events-by-type (group-by :type-name milestone-events)
                     
                     facts (d/get-doi-facts doi)
                     
                     facts-by-type (group-by :type-name facts)
                     
                     all-dates (sort t/before? (map :event events))
                     first-date (first all-dates)
                     last-date (last all-dates)
                     
                     ;; add 1 day of padding either side for charting
                     first-date-pad (t/minus first-date (t/days 1))
                     last-date-pad (t/plus last-date (t/days 1))
                     
                     render-context {:first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :doi doi
                                     :events events
                                     :milestone-events milestone-events
                                     :continuous-events continuous-events
                                     :continuous-events-by-type continuous-events-by-type
                                     :milestone-events-by-type milestone-events-by-type
                                     :facts facts
                                     :facts-by-type facts-by-type}]
               (render-file "templates/doi.html" render-context))))

(defresource domains-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [domain (-> ctx :request :params :domain)]
            [domain {::domain domain}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/domains/" (::domain ctx))))))

(defresource domain-page
  [domain]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [events (d/get-domain-events domain)
                                          
                     events-by-type (group-by :type-name events)
                                          
                     all-dates (sort t/before? (map :event events))
                     first-date (first all-dates)
                     last-date (last all-dates)
                     
                     ;; add 1 day of padding either side for charting
                     first-date-pad (t/minus first-date (t/days 1))
                     last-date-pad (t/plus last-date (t/days 1))
                     
                     render-context {:first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :domain domain
                                     :events events
                                     :events-by-type events-by-type}]
               (render-file "templates/domain.html" render-context))))

(defresource subdomains-redirect
  []
  :available-media-types ["text/html"]
  :exists? (fn [ctx] (let [subdomain (-> ctx :request :params :subdomain)]
            [subdomain {::subdomain subdomain}]))
  :handle-ok (fn [ctx]
               (ring-response (redirect (str "/subdomains/" (::subdomain ctx))))))

(defresource subdomain-page
  [subdomain]
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
               (let [events (d/get-subdomain-events subdomain)
                                          
                     events-by-type (group-by :type-name events)
                                          
                     all-dates (sort t/before? (map :event events))
                     first-date (first all-dates)
                     last-date (last all-dates)
                     
                     ;; add 1 day of padding either side for charting
                     first-date-pad (t/minus first-date (t/days 1))
                     last-date-pad (t/plus last-date (t/days 1))
                     
                     other-subdomains (d/get-other-subdomains subdomain)
                     
                     render-context {:first-date first-date
                                     :last-date last-date
                                     :first-date-pad first-date-pad
                                     :last-date-pad last-date-pad
                                     :subdomain subdomain
                                     :events events
                                     :events-by-type events-by-type
                                     :other-subdomains other-subdomains}]
               (render-file "templates/subdomain.html" render-context))))
(defroutes app-routes
  (GET "/" [] (home))
  (context "/dois" []
    (GET "/" [] (dois-redirect))
    (context ["/:doi-prefix/:doi-suffix" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-page doi-prefix doi-suffix)))
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
  
  (route/resources "/")
  )

(def app
  (-> app-routes
      handler/site
      ))
