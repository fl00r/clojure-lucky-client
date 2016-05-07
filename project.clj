(defproject suvlucky-client "0.1.13"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.zeromq/jeromq "0.3.5"]
                 [org.clojure/core.async "0.2.374"]
                 [com.stuartsierra/component "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [org.slf4j/slf4j-api "1.7.7"]
                                  [ch.qos.logback/logback-classic "1.1.2"]]}})
