;; QCAS.lisp -- Query CAS. Return the value of the ref cell prior to CAS.
;;
;; QCAS tries to simulate the effect of the X86 CMPXCHG instruction
;; which provides both a boolean success/fail flag, and the results of the
;; ref prior to attempted CAS.
;;
;; DM/RAL  02/17
;; -------------------------------------------------------------

(defpackage #:qcas
  (:use #:common-lisp)
  (:import-from #:ref
   #:ref-value
   #:cas)
  (:export
   #:qcas
   #:aqcas
   ))

(in-package #:qcas)
   
(declaim (optimize (speed 3) (safety 0) (float 0)))

;; -----------------------------------------------------------------
;; QCAS -- (Query CAS) A CAS with a returned value of the actual
;; contents of a ref before CAS success/failure

(defvar +qcas-tryagain+  (gensym))

(defstruct qcas
  old new sav)

(defun QCAS (ref old new)
  (labels ((do-qcas ()
             (let ((desc (make-qcas
                          :old  old
                          :new  new)))
               (with-accessors ((sav qcas-sav)) desc
                 (hcl:unwind-protect-blocking-interrupts-in-cleanups
                     (progn
                       (loop until (cas ref (setf sav (ref-value ref)) desc))
                       (qcas-help desc))
                   (cas ref desc sav))
                 ))))
    
    (multiple-value-bind (tf v)
        (do-qcas)
      (cond ((eq tf +qcas-tryagain+)
             (qcas ref old new))
            
            (t
             (values tf v))
            ))))

(defun qcas-help (desc)
  (let ((old  (qcas-old desc))
        (new  (qcas-new desc)))
    (with-accessors ((sav qcas-sav)) desc
      (cond ((eq sav old)
             (setf sav new)
             (values t old))
            
            ((qcas-p sav)
             (multiple-value-bind (tf v)
                 (qcas-help sav)
               (cond (tf
                      (setf sav (qcas-sav sav))
                      +qcas-tryagain+)
                     
                     ((eq old v)
                      (setf sav new)
                      (values t old))
                     
                     (t
                      (setf sav (qcas-sav sav))
                      (values nil v))
                     )))
            
            (t
             (values nil sav))
            ))))
               
;; -----------------------------------------------------------------
;; AQCAS -- (Approximate Query CAS) A CAS with a returned value of the
;; approximate contents of a ref before CAS success/failure

;; this version returns only an estimate of the prior ref contents
(defun AQCAS (ref old new)
  ;; approximate QCAS
  (let ((sav  (ref-value ref)))
    (cond ((cas ref old new)
           (values t old)) ;; only time we actually know

          ((eq sav old)
           (aqcas ref old new)) ;; oops! try again

          (t
           (values nil sav)) ;; return our estimate
          )))
  