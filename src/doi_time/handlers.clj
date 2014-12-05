(ns doi-time.handlers
  (:require [doi-time.data :as d]
            [doi-time.db :as db])
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
                (let [dois-input (-> ctx :request :params  :upload :tempfile slurp)
                      dois (.split dois-input "\r?\n" )
                      results (map (fn [doi]
                                        (when doi (export-info (d/get-doi-info doi)))) dois)]
                  (remove nil? results))))

(defresource article
  [doi-prefix doi-suffix]
  :allowed-methods [:get]
  :available-media-types ["text/html" "application/json" "text/csv"]
  :exists? (fn [ctx]
            (let [doi (str doi-prefix "/" doi-suffix)
                  info (d/get-doi-info doi)]
              [info {::info info}]))
  :handle-ok (fn [ctx]
              (export-info (::info ctx))))

(defn home [] (prn "TEST")
                   )

(defroutes app-routes
  (GET "/" [] (redirect "https://github.com/CrossRef/doi-time/blob/master/README.md"))
    (context "/articles" []
      (ANY "/" [] (articles))
          (context ["/:doi-prefix/:doi-suffix" :doi-prefix #".+?" :doi-suffix #".+?"] [doi-prefix doi-suffix] (article doi-prefix doi-suffix))))

(def app
  (-> app-routes
      handler/site))
