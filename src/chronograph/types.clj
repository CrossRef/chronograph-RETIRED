(ns chronograph.types)

(def sources [{:name :CrossRefMetadata :description "CrossRef Metadata"}
              {:name :CrossRefLogs :description "CrossRef Resolution Logs"}
              {:name :CrossRefRobot :description "CrossRef Robot"}
              {:name :CrossRefDeposit :description "CrossRef Deposit System"}
              {:name :Cocytus :description "Wikipedia Cocytus"}])


(def source-names (set (map :name sources)))
(def sources-by-name (into {} (map (fn [src] [(:name src) src]) sources)))

; The :format function takes an event and returns a map that describes the event.
; Unfortunately these functions need to be late-bound so that they can be included in the below maps but refer to the maps's values.

(def arg123 [:arg1 :arg2 :arg3])

(defn standard-format [event]
  (let [; type will point to one of the below
        type (:type event)
        
        ; go from event.arg1 and event.type.arg1 to name => value
        substitute-arg-names (into {} (map (fn [arg-name]
                                             (when-let [value (get event arg-name)]
                                               [(-> event :type arg-name) value])) arg123))]
    
    substitute-arg-names))

(def types [
    {:name :issued
     :description "Publisher Issue date"
     :arg1 "Date supplied by publisher"
     :storage :milestone
     :format standard-format}
    {:name :deposited
     :description "Publisher first deposited with CrossRef"
     :storage :milestone
     :format standard-format}
    {:name :updated
     :description "Publisher most recently updated CrossRef metadata"
     :storage :milestone
     :format standard-format}
    {:name :first-resolution-test
     :description "First attempt DOI resolution test"
     :arg1 "Initial resolution URL"
     :arg2 "Ultimate resolution URL"
     :arg3 "Number of redirect hops"
     :storage :milestone
     :format standard-format}
    {:name :WikipediaCitation
     :description "Citation in Wikipedia"
     :arg1 "Action"
     :arg2 "Page URL"
     :arg3 "Timestamp"
     :storage :event
     :format standard-format}
    {:name :first-resolution
     :description "First DOI resolution"
     :storage :milestone
     :format standard-format}
    {:name :total-resolutions
     :description "Total resolutions count"
     :storage :fact
     :format standard-format}
    {:name :daily-resolutions
     :description "Daily resolutions count"
     :storage :timeline
     :format standard-format}
    {:name :monthly-resolutions
     :description "Monthly resolutions count"
     :storage :timeline
     :format standard-format}
    {:name :yearly-resolutions
     :description "Yearly resolutions count"
     :storage :timeline
     :format standard-format}
    {:name :daily-referral-domain
     :description "Daily referral count from domain"
     :storage :timeline
     :format standard-format}
    {:name :monthly-referral-domain
     :description "Monthly referral count from domain"
     :storage :timeline
     :format standard-format}
    {:name :yearly-referral-domain
     :description "Yearly referral count from domain"
     :storage :timeline
     :format standard-format}
    {:name :total-referrals-domain
     :description "Total referrals count from domain"
     :storage :fact
     :format standard-format}
    {:name :daily-referral-subdomain
     :description "Daily referral count from subdomain"
     :storage :timeline
     :format standard-format}
    {:name :monthly-referral-subdomain
     :description "Monthly referral count from subdomain"
     :storage :timeline
     :format standard-format}
    {:name :yearly-referral-subdomain
     :description "Yearly referral count from subdomain"
     :storage :timeline
     :format standard-format}
    {:name :total-referrals-subdomain
     :description "Total referrals count from subdomain"
     :storage :fact
     :format standard-format}
    {:name :crossmark-update-published
     :description "CrossMark Update to this DOI Published"
     :storage :milestone
     :arg1 "DOI of update"
     :format standard-format}])

(def type-names (set (map :name types)))

(def types-by-name (into {} (map (fn [typ] [(:name typ) typ]) types)))

(defn type-info [event]
  "Generate extra info for an event according to its type"
  ((-> event :type :format) event))

(defn export-type-info
  "Tidy up an event removing type stuff so it can be cleanly exported"
  [event]
  ; type has unserializable functions etc
  ; remove the type completely, replace with name
  (-> event
    (assoc :type-info (type-info event)
                 :type-name (-> event :type :name)
                 :type-description (-> event :type :description))
    (dissoc :type)))
      

; Different storage formats for different kinds of data.
; Timeline is for pre-aggregated per-period info like resolutions per day.
; Milestone is for one-off events, like publication date.
; Event is for repeated events, like citations.
; Fact is for non-dated figures, like total citation count.

; timeline: (type, doi, serialised {date -> count}, source). Unique (doi, type, source).
; milestone: (type, doi, date, count, source). Unique (type, doi, source).
; event: (type, doi, date, count, source).
; fact: (type, doi, count, source)
(def storage-formats #{:timeline :event :milestone :fact})

(def conflict-methods #{:replace :newer :older})