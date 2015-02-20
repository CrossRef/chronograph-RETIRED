(defproject chronograph "0.1.0-SNAPSHOT"
  :description "DOI time! Chronological information about DOIs"
  ; :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                  [crossref-util "0.1.7"]
                  [clj-http "1.0.1"]
                  [clj-time "0.8.0"]
                  [org.clojure/data.json "0.2.5"]
                  [compojure "1.3.0"]
                  [http-kit "2.1.16"]
                  [org.clojure/java.jdbc "0.3.6"]
                  [korma "0.3.0"]
                  [mysql-java "5.1.21"]
                  [compojure "1.3.1"]
                  [liberator "0.12.2"]
                  [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                  [robert/bruce "0.7.1"]
                  [selmer "0.7.7"]
                  [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                  [org.clojure/data.csv "0.1.2"]
                  [ring "1.3.2"]                  
                  ]
  ; :global-vars {*warn-on-reflection* true} 
  :main ^:skip-aot chronograph.main
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=UTC"]
  :profiles {:uberjar {:aot :all}})
