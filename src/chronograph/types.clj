(ns chronograph.types)

(def sources [{:name :CrossRefMetadata :description "CrossRef Metadata"}
              {:name :CrossRefLogs :description "CrossRef Resolution Logs"}
              {:name :CrossRefRobot :description "CrossRef Robot"}
              {:name :CrossRefDeposit :description "CrossRef Deposit System"}
              {:name :Cocytus :description "Wikipedia Cocytus"}])


(def source-names (set (map :name sources)))
(def sources-by-id (into {} (map (fn [src] [(:name src) src]) sources)))

; Merge-methods


(def types [
    {:name :issued
     :description "Publisher Issue date"
     :arg1 "Date supplied by publisher"
     :storage :milestone}
    {:name :deposited
     :description "Publisher first deposited with CrossRef"
     :storage :milestone}
    {:name :updated
     :description "Publisher most recently updated CrossRef metadata"
     :storage :milestone}
    {:name :first-resolution-test
     :description "First attempt DOI resolution test"
     :arg1 "Initial resolution URL"
     :arg2 "Ultimate resolution URL"
     :arg3 "Number of redirect hops"
     :storage :milestone}
    {:name :WikipediaCitation
     :description "Citation in Wikipedia"
     :arg1 "Action"
     :arg2 "Page URL"
     :arg3 "Timestamp"
     :storage :event}
    {:name :first-resolution
     :description "First DOI resolution"
     :storage :milestone}
    {:name :total-resolutions
     :description "Total resolutions count"
     :storage :milestone}
    {:name :daily-resolutions
     :description "Daily resolutions count"
     :storage :timeline}
    {:name :monthly-resolutions
     :description "Monthly resolutions count"
     :storage :timeline}
    {:name :yearly-resolutions
     :description "Yearly resolutions count"
     :storage :timeline}
    {:name :daily-referral-domain
     :description "Daily referral count from domain"
     :storage :timeline}
    {:name :monthly-referral-domain
     :description "Monthly referral count from domain"
     :storage :timeline}
    {:name :yearly-referral-domain
     :description "Yearly referral count from domain"
     :storage :timeline}
    {:name :total-referrals-domain
     :description "Total referrals count from domain"
     :storage :milestone}
    {:name :daily-referral-subdomain
     :description "Daily referral count from subdomain"
     :storage :timeline}
    {:name :monthly-referral-subdomain
     :description "Monthly referral count from subdomain"
     :storage :timeline}
    {:name :yearly-referral-subdomain
     :description "Yearly referral count from subdomain"
     :storage :timeline}
    {:name :total-referrals-subdomain
     :description "Total referrals count from subdomain"
     :storage :timeline}
    {:name :crossmark-update-published
     :description "CrossMark Update to this DOI Published"
     :storage :milestone
     :arg1 "DOI of update"}])

(def type-names (set (map :name types)))
; TODO RENAME TO types-by-name
(def types-by-id (into {} (map (fn [typ] [(:name typ) typ]) types)))

; Different storage formats for different kinds of data.
; These are stored in the context of the type
; timeline: serialised {date -> count}, source
; event: date -> count, source
; milestone: date, source
(def storage-formats #{:timeline :event :milestone})

(def conflict-methods #{:replace :newer :older})