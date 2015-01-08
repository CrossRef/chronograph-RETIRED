(ns chronograph.core
  (:require [clj-time.core :as t]
            [clj-time.format :refer [parse formatter unparse]]))

(def yyyy-mm-dd (formatter (t/default-time-zone) "yyyy-MM-dd" "yyyy-MM-dd"))