(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def lib 'io.github.hive-agi/hive-events)
(def version (str/trim (slurp "VERSION")))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(def readme "README.md")

(defn coord-patterns
  "Regexes matching the README install coordinates as (prefix)(version)(suffix).
   Derived from `lib`, so a group/artifact rename can't silently orphan them."
  []
  (let [l (java.util.regex.Pattern/quote (str lib))]
    [;; deps.edn:   io.github.hive-agi/hive-events {:mvn/version "X"}
     (re-pattern (str "(" l " \\{:mvn/version \")([^\"]+)(\")"))
     ;; Leiningen:  [io.github.hive-agi/hive-events "X"]
     (re-pattern (str "(\\[" l " \")([^\"]+)(\")"))]))

(defn readme-versions
  "Every version currently pinned in the README's install coordinates."
  [source]
  (into [] (mapcat #(map second (re-seq % source))) (coord-patterns)))

(defn- sync-readme
  "Rewrite the README install coordinates to `version`. Returns the new source."
  [source]
  (reduce (fn [s re] (str/replace s re (str "$1" version "$3")))
          source
          (coord-patterns)))

(def pom-data
  [[:description "Async event system for Clojure — re-frame patterns (events, effects, coeffects, interceptors, sagas) for the JVM, built on core.async."]
   [:url "https://github.com/hive-agi/hive-events"]
   [:licenses
    [:license
     [:name "MIT"]
     [:url "https://opensource.org/license/mit"]]]
   [:scm
    [:url "https://github.com/hive-agi/hive-events"]
    [:connection "scm:git:git://github.com/hive-agi/hive-events.git"]
    [:developerConnection "scm:git:ssh://git@github.com/hive-agi/hive-events.git"]
    [:tag (str "v" version)]]
   [:developers
    [:developer
     [:name "Pedro G. Branquinho"]]]])

(defn clean [_]
  (b/delete {:path "target"}))

(defn sync-version
  "Propagate the canonical top-level VERSION to everything that restates it:
   currently just the README install coordinates."
  [_]
  (let [before (slurp readme)
        after  (sync-readme before)]
    (when (not= before after)
      (spit readme after))
    (println (str "Synced " readme " install coords -> " version
                  (when (= before after) " (already current)")))))

(defn jar
  "Build the library thin jar + pom for Clojars/Maven consumption."
  [_]
  (clean nil)
  (sync-version nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]
                :pom-data pom-data})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file})
  (println (str "Built " jar-file)))

(defn deploy
  "Deploy the library jar to Clojars. Requires CLOJARS_USERNAME + CLOJARS_PASSWORD
   (a Clojars deploy token) in the environment."
  [_]
  (jar nil)
  ((requiring-resolve 'deps-deploy.deps-deploy/deploy)
   {:installer :remote
    :artifact  jar-file
    :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
  (println (str "Deployed " lib " " version " to Clojars")))

(defn- already-published-private?
  "True if this exact lib+version pom is already in the private Gitea registry.
   Authed HEAD — private-org reads require credentials."
  [url username token]
  (let [[grp art] (str/split (str lib) #"/")
        pom-url (format "%s/%s/%s/%s/%s-%s.pom"
                        url (str/replace grp "." "/") art version art version)
        auth (str "Basic " (.encodeToString (java.util.Base64/getEncoder)
                                            (.getBytes (str username ":" token))))]
    (try
      (let [conn (doto ^java.net.HttpURLConnection (.openConnection (java.net.URL. pom-url))
                   (.setRequestMethod "HEAD")
                   (.setRequestProperty "Authorization" auth)
                   (.setConnectTimeout 10000)
                   (.setReadTimeout 10000))]
        (= 200 (.getResponseCode conn)))
      (catch Throwable _ false))))

(defn deploy-private
  "Build + deploy to the private Gitea Maven registry (hive-agi org).
   Env: GITEA_MAVEN_TOKEN (required, non-blank), GITEA_MAVEN_USERNAME (default buddhilw),
   GITEA_MAVEN_URL (default https://gitea.hive-mcp.com/api/packages/hive-agi/maven).
   No-ops when this version already exists in the registry (idempotent)."
  [_]
  (let [env (fn [k fallback]
              (let [v (System/getenv k)]
                (if (str/blank? v) fallback v)))
        url (env "GITEA_MAVEN_URL" "https://gitea.hive-mcp.com/api/packages/hive-agi/maven")
        username (env "GITEA_MAVEN_USERNAME" "buddhilw")
        token (System/getenv "GITEA_MAVEN_TOKEN")]
    (when (str/blank? token)
      (throw (ex-info "GITEA_MAVEN_TOKEN is required (non-blank)" {:env "GITEA_MAVEN_TOKEN"})))
    (if (already-published-private? url username token)
      (println "Skip:" (str lib) version "already in private registry — bump VERSION to release.")
      (do
        (jar nil)
        ((requiring-resolve 'deps-deploy.deps-deploy/deploy)
         {:installer  :remote
          :artifact   jar-file
          :pom-file   (b/pom-path {:lib lib :class-dir class-dir})
          :repository {"gitea" {:url url :username username :password token}}})
        (println "Deployed" (str lib) version "to" url)))))
