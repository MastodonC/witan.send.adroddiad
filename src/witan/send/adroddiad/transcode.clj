(ns witan.send.adroddiad.transcode
  (:import
   [java.awt RenderingHints]
   [java.io File FileOutputStream ByteArrayInputStream ByteArrayOutputStream]
   [org.apache.batik.anim.dom SAXSVGDocumentFactory]
   [org.apache.batik.transcoder TranscoderInput TranscoderOutput]
   [org.apache.batik.transcoder.image PNGTranscoder]))

(defn file-str [filename]
  (-> filename
      File.
      .toURL
      .toString))

(def svg-parser (SAXSVGDocumentFactory. "org.apache.xerces.parsers.SAXParser"))

(defn svg-file->document [parser file-string]
  (.createDocument parser file-string))

(defn- high-quality-png-transcoder []
  (proxy [PNGTranscoder] []
    (createRenderer []
      (let [add-hint (fn [hints k v] (.add hints (RenderingHints. k v)))
            renderer (proxy-super createRenderer)
            ;;hints    (.getRenderingHints renderer)
            hints (RenderingHints. RenderingHints/KEY_ALPHA_INTERPOLATION RenderingHints/VALUE_ALPHA_INTERPOLATION_QUALITY)]
        (doto hints
          (add-hint RenderingHints/KEY_ALPHA_INTERPOLATION RenderingHints/VALUE_ALPHA_INTERPOLATION_QUALITY)
          (add-hint RenderingHints/KEY_INTERPOLATION       RenderingHints/VALUE_INTERPOLATION_BICUBIC)
          (add-hint RenderingHints/KEY_ANTIALIASING        RenderingHints/VALUE_ANTIALIAS_ON)
          (add-hint RenderingHints/KEY_COLOR_RENDERING     RenderingHints/VALUE_COLOR_RENDER_QUALITY)
          (add-hint RenderingHints/KEY_DITHERING           RenderingHints/VALUE_DITHER_DISABLE)
          (add-hint RenderingHints/KEY_RENDERING           RenderingHints/VALUE_RENDER_QUALITY)
          (add-hint RenderingHints/KEY_STROKE_CONTROL      RenderingHints/VALUE_STROKE_PURE)
          (add-hint RenderingHints/KEY_FRACTIONALMETRICS   RenderingHints/VALUE_FRACTIONALMETRICS_ON)
          (add-hint RenderingHints/KEY_TEXT_ANTIALIASING   RenderingHints/VALUE_TEXT_ANTIALIAS_OFF))
        (.setRenderingHints renderer hints)
        renderer))))

(defn svg-document->png-file [svg-document filename]
  (with-open [out-stream (FileOutputStream. filename)]
    (let [in (TranscoderInput. svg-document)
          out (TranscoderOutput. out-stream)
          trans (high-quality-png-transcoder)]
      (.transcode trans in out))))
