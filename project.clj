(defproject doi-time "0.1.0-SNAPSHOT"
  :description "DOI time! Chronological information about DOIs"
  ; :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                  [crossref-util "0.1.6"]
                  [clj-http "1.0.1"]
                  [clj-time "0.8.0"]
                  [org.clojure/data.json "0.2.5"]
                  [compojure "1.3.0"]
                  [http-kit "2.1.16"]
                  [org.clojure/java.jdbc "0.3.6"]
                  [korma "0.3.0"]
                  [mysql-java "5.1.21"]
                  [compojure "1.3.1"]
                  [liberator "0.12.2"]]
  :plugins [[lein-ring "0.8.13"]]
  :ring {:handler doi-time.handlers/app}
  :main ^:skip-aot doi-time.main
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=UTC"]
  :profiles {:uberjar {:aot :all}})
