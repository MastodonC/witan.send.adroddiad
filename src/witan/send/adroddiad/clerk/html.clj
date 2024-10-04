(ns witan.send.adroddiad.clerk.html
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]))

(defn ns->filepath
  "Given a namespace (name) `ns` and (optional) `project-path`
   (which may be specified without a trailing \"/\" as in `deps.edn`),
   returns the path (relative to its project root) of the corresponding `*.clj` file,
   by replacing . with / & - with _. and prepending the `project-path`."
  [ns project-path]
  (str project-path
       (if (str/ends-with? project-path "/") nil "/")
       (str/replace ns #"\.|-" {"." "/", "-" "_"})
       ".clj"))

(defn build-ns!
  "Build static HTML file from \"*.clj\" file corresponding to namespace (named) `ns`.
   Optional trailing keyword arguments specify:
   - `project-path`: the path (relative to the project root) under which
                     the namespace \"*.clj\" file resides..
                     (May be specified without a trailing \"/\" as in `deps.edn`.)
                     Defaults to \"notebooks\".
   - `out-dir`     : the path under which the built HTML should be saved.
                     (May be specified without a trailing \"/\".)
                     Defaults to the directory containing the namespace \"*.clj\" file.
   - `out-filename`: the filename (relative to `out-dir`) for the HTML file.
                     (May be specified without the \".html\" extension.)
                     (May be specified with leading sub-directories.)
                     Defaults to the last component of the namespace name."
  [ns & {:keys [project-path out-dir out-filename]
         :or   {project-path "notebooks"}}]
  (let [ns-filepath  (ns->filepath ns project-path)
        out-dir      (or out-dir (str/replace ns-filepath #"\/[^\/]+$" "/"))
        out-filename (or out-filename (str/replace ns #"^.*\." ""))
        out-filepath (str out-dir
                          (if (str/ends-with? out-dir "/") nil "/")
                          out-filename
                          (if (str/ends-with? out-filename ".html") nil ".html"))]
    #_{:ns-filepath  ns-filepath
       :out-dir      out-dir
       :out-filename out-filename
       :out-filepath out-filepath}
    (io/make-parents out-filepath)
    (clerk/build! {:paths      [ns-filepath]
                   :ssr        true
                   #_#_:bundle true         ; clerk v0.15.957
                   :package    :single-file ; clerk v0.16.1016
                   :out-path   "."})
    (.renameTo (io/file "./index.html") (io/file out-filepath))))

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
  "Build static HTML file from the \"*.clj\" file corresponding to namespace (named) `ns`,
   saving the HTML file under `out-dir` with filepath (relative to `out-dir`) derived
   from the namespace name turned into a path (i.e. with \".\" replaced by \"/\")."
  [out-dir ns]
  (build-ns! ns {:out-dir      out-dir
                 :out-filename (ns-to-html-file-name "" *ns*)}))

(comment ;;; # Example usage
  ;; Build static HTML of current notebook namespace (assumed to be derived
  ;; from a *.clj file in the "notebooks" project-path) and save the
  ;; resulting HTML file alongside the *.clj with filename matching the
  ;; leaf of the namespace name (i.e. matching the *.clj file name but with
  ;; "_" replaced with "-"):
  (build-ns! *ns*)

  ;; Build static HTML of current notebook namespace and save in
  ;; "./wp-2-16-1/" (relative to the project root) with filename
  ;; matching the leaf of the namespace name:
  (build-ns! *ns* :out-dir "./wp-2-16-1/")

  ;; Where the first part of the namespace name matches the desired output
  ;; directory, `root-ns-to-out-dir` may be used to extract the `out-dir`:
  (def out-dir (root-ns-to-out-dir *ns*))

  ;; Build static HTML of current notebook namespace and save under `out-dir`,
  ;; with path to the HTML file relative to `out-dir` derived from the full
  ;; namespace name turned into a path (i.e. with "." replaced with "/"):
  (build-ns! ns {:out-dir      out-dir
                 :out-filename (ns-to-html-file-name "" *ns*)})
  ;; or:
  (ns->html out-dir *ns*)

  )
