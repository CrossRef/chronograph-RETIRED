(ns chronograph.handlers
  (:require [chronograph.data :as d]
            [chronograph.db :as db])
  (:require [compojure.core :refer [context defroutes GET ANY]]
            [compojure.handler :as handler])
  (:require [ring.util.response :refer [redirect]])
  (:require [liberator.core :refer [defresource resource]]
            [liberator.representation :refer [ring-response]]))

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

(defresource articles
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

(defroutes app-routes
  (GET "/" [] (redirect "https://github.com/CrossRef/chronograph/blob/master/README.md"))
    (context "/dois" []
      (ANY "/" [] (articles))
          (context ["/:doi-prefix/:doi-suffix/facts" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-facts doi-prefix doi-suffix))
          (context ["/:doi-prefix/:doi-suffix/events" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (doi-events doi-prefix doi-suffix))
          ))

(def app
  (-> app-routes
      handler/site))
