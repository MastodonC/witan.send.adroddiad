(ns witan.send.adroddiad.sen2
  "Special educational needs survey (SEN2) information

  from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey) unless stated otherwise.

  See in particular:
  - [Special educational needs survey: guide to submitting data](https://www.gov.uk/guidance/special-educational-needs-survey)
  - [Special educational needs person level survey 2023: guide](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-2023-guide)
  - [Special educational needs person level survey: technical specification](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-technical-specification)"
  )

(def census-dates
  "SEN2 census dates"
  (sorted-map
   2015 "2015-01-15" ; from [SEN2 2015 Guide v1.3](https://dera.ioe.ac.uk/21852/1/SEN2_2015_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2016 "2016-01-21" ; from [SEN2 2016 Guide v1.3](https://dera.ioe.ac.uk/24874/1/SEN2_2016_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2017 "2017-01-19" ; from [SEN2 2017 Guide v1.2](https://dera.ioe.ac.uk/27646/1/SEN2_2017_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2018 "2018-01-18" ; from [SEN2 2018 Guide v1.2](https://dera.ioe.ac.uk/30003/1/SEN2_2018_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2019 "2019-01-17" ; from [SEN2 2019 Guide v1.2](https://dera.ioe.ac.uk/32271/1/SEN2_2019_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2020 "2020-01-16" ; from [SEN2 2020 Guide v1.1](https://dera.ioe.ac.uk/34219/1/SEN2_2020_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2021 "2021-01-14" ; from [SEN2 2021 Guide v1.1](https://dera.ioe.ac.uk/36468/1/SEN2_2021_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2022 "2022-01-20" ; from [SEN2 2022 Guide v1.1](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf) (retrieved 2023-04-12)
   2023 "2023-01-19" ; from [SEN2 2023 Guide v1.0](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1099346/2023_SEN2_Person_level_-_Guide_Version_1.0.pdf) (retrieved 2023-04-12)
   ))


;;; # EHCP needs <SENtype>
;; Definitions and functions for handling EHCP needs <SENtype>,
;; per item 5.6 of the DfE 2023 SEN2 return.
(def need->order
  "Map EHCP need abbreviations to order"
  (let [m (zipmap ["SPLD" "MLD" "SLD" "PMLD" "SEMH" "SLCN" "HI" "VI" "MSI" "PD" "ASD" "OTH"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def needs
  "EHCP need abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get need->order k1) k1]
                              [(get need->order k2) k2]))
         (keys need->order)))

(def need->label
  "Map EHCP need abbreviation to label for display"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get need->order k1) k1]
                                             [(get need->order k2) k2])))
        {"SPLD" "Specific learning difficulty"
         "MLD"  "Moderate learning difficulty"
         "SLD"  "Severe learning difficulty"
         "PMLD" "Profound and multiple learning difficulty"
         "SEMH" "Social, emotional and mental health"
         "SLCN" "Speech, language and communication needs"
         "HI"   "Hearing impairment"
         "VI"   "Vision impairment"
         "MSI"  "Multi-sensory impairment"
         "PD"   "Physical disability"
         "ASD"  "Autistic spectrum disorder"
         "OTH"  "Other difficulty"}))

;;; # SEN Setting <SENsetting>
;; Definitions and functions for handling SEN Setting - Establishment Type <SENsetting>,
;; per item 5.5c of the DfE 2023 SEN2 return.
(def sen-setting->order
  "Map SEN setting abbreviations to order"
  (let [m (zipmap ["OLA" "OPA" "EHE" "EYP" "NEET" "NIEC" "NIEO"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def sen-settings
  "SEN setting abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                              [(get sen-setting->order k2) k2]))
         (keys sen-setting->order)))

(def sen-setting->label
  "Map SEN setting abbreviation to label for display"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                                             [(get sen-setting->order k2) k2])))
        {"OLA" "Other LA Arrangements (inc. EOTAS)"
         "OPA" "Other Parent/Person Arrangements (exc. EHE)"
         "EHE" "Elective Home Education"
         "EYP" "Early Years Provider"
         "NEET" "Not in Education, Training or Employment"
         "NIEC" "Ceasing"
         "NIEO" "Other"}))

(def sen-setting->description
  "Map SEN setting abbreviation to description"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                                             [(get sen-setting->order k2) k2])))
        {"OLA"  (str "Other – arrangements made by the local authority "
                     "in accordance with section 61 of the Children and Families Act 2014, "
                     "(\"education otherwise than at a school or post-16 institution etc\").")
         "OPA"  (str "Other – alternative arrangements made by parents or young person "
                     "in accordance with section 42(5) of the Children and Families Act 2014, "
                     "excluding those who are subject to elective home education.")
         "EHE"  (str "Elective home education – alternative arrangements made by parents or young person "
                     "in accordance with section 42(5) of the Children and Families Act 2014, for elective home education.")
         "EYP"  (str "Early years provider with no GIAS URN "
                     "(for example private nursery, independent early years providers and childminders).")
         "NEET" (str "Not in education, training or employment (aged 16-25).")
         "NIEC" (str "Not in education or training – Notice to cease issued.")
         "NIEO" (str "Not in education – Other – "
                     "Where this is used, the local authority will be prompted for further information in COLLECT, "
                     "for example, transferred into the local authority with an EHC plan and awaiting placement.")}))
