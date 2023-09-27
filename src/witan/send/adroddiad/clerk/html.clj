(ns witan.send.adroddiad.clerk.html
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]))

(defn root-ns-to-out-dir 
  "Convert the first part of `ns` to a path name."
  [ns]
  (->> ns 
       str
       (re-find #"^[^\.]*")
       (format "./%s/")))

(defn pathified-namespace 
  "Turn . into / and - into _"
  [ns]
  (str/replace ns #"\.|-" {"." "/" "-" "_"}))

(defn ns-to-html-file-name
  "Append out-dir to a converted namespace for a nice html name."
  [out-dir ns]
  (->> (str/replace ns #"\." {"." "/"})
       (format "%s%s.html" out-dir)))

(defn ns->html
  "Output the current namespace as an html file in a directory where the
  non-ending bits of the namespace are a file path."
  [out-dir ns]
  (let [in-path   (str "notebooks/" (pathified-namespace ns) ".clj")
        out-path  (ns-to-html-file-name out-dir ns)
        index-out (str out-dir "index.html")]
    (io/make-parents out-path)
    (clerk/build! {:paths    [in-path]
                   :ssr      true
                   :bundle   true
                   :out-path out-dir})
    (.renameTo (io/file index-out) (io/file out-path))))

(def out-dir (root-ns-to-out-dir *ns*))

(comment

  (def out-dir (root-ns-to-out-dir *ns*))

  (ns->html out-dir *ns*)
  
  )
