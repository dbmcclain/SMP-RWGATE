
(asdf:defsystem "cas-rwgate"
  :description "cas-rwgate: an SMP-safe many reader / single writer gate"
  :version     "1.0"
  :author      "D.McClain <dbm@refined-audiometrics.com>"
  :license     "Copyright (c) 2017 Refined Audiometrics Laboratory, LLC. All rights reserved."
  :components  ((:file "ref")
                (:file "mcas")
                (:file "cas-rwgate")
                )
  :serial t
  :depends-on   (
                 ))

