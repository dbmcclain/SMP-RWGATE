;; MCAS.lisp -- Multiple CAS on CAR/CDR of ref-cells.
;;
;; Adapted from UCAM-CL-TR-579 U.Cambridge Tech Report 579,
;; "Practical lock-freedom" by Keir Fraser, Feb 2004
;;
;; DM/RAL  02/17
;; -------------------------------------------------------------

(defpackage #:mcas
  (:use #:common-lisp)
  (:import-from #:ref
   #:ref
   #:ref-value
   #:car-ref
   #:cdr-ref
   #:cas)
  (:export
   #:qcas
   #:%mcas
   #:mcas
   #:mcas-read
   ))

(in-package #:mcas)
   
(declaim (optimize (speed 3) (safety 0) (float 0)))

;; -----------------------------------------------------------------
;; AQCAS -- (Approximate Query CAS) A CAS with a returned value of the
;; approximate contents of a ref before CAS success/failure

#||#
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
#||#
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
               
;; ------------------
;; CCAS - Conditional CAS
;;
;; The job of CCAS is to conditionally acquire a reference on behalf
;; of an MCAS operation. The condition is that the MCAS operation must
;; still be in a state of :UNDECIDED.
;;
;; If this condition is met, we either acquire the ref cell with the
;; MCAS descriptor, or we fail because the ref cell did not contain
;; the expected old value
;;
;; If the CCAS acquires, but the condition is not met, it can only be
;; because another thread pushed us along to a :FAILED or :SUCCEEDED
;; resolution already. In other words we have already been through here.
;;

(defstruct mcas-desc
  triples status)

(defmacro mstatus (mdesc)
  `(car (mcas-desc-status ,mdesc)))

(defstruct ccas-desc
  ref old mdesc)


(defun ccas (ref old mdesc)
  (declare (mcas-desc mdesc))
  
  (labels ((try-ccas (desc)
             (declare (ccas-desc desc))
             (multiple-value-bind (t/f v)
                 (aqcas ref old desc)
               (cond (t/f
                      ;;
                      ;;               CAS succeeded
                      ;;                     |  
                      ;;              ref-val EQ old -> CCAS desc
                      ;;               |     |    |                   
                      ;;   MCAS :UNDECIDED   |   MCAS :FAILED
                      ;;                     |
                      ;;         MCAS :SUCCEEDED && old EQ new
                      ;;
                      ;; We got it! Either this is the first time
                      ;; through with MCAS :UNDECIDED or, because the
                      ;; old value was eq the expected old, the MCAS was
                      ;; pushed along by another thread and the state
                      ;; must now be :FAILED.
                      ;;
                      ;; (Or else, the planned new value was the same as
                      ;; the old value and MCAS :SUCCEEDED. Either way,
                      ;; it put back the old value.).
                      ;;
                      ;; In the first case, we can now replace our CCAS
                      ;; descriptor with the caller's MCAS descriptor.
                      ;;
                      ;; In the second case, we must put back the old value.
                      ;;
                      (ccas-help desc)
                      (values t v))

                     ((ccas-desc-p v)
                      (ccas-help v)
                      (try-ccas desc))

                     (t
                      (values nil v))
                     ))))
    
    (try-ccas (make-ccas-desc
               :ref      ref
               :old      old
               :mdesc    mdesc))
    ))

(defun ccas-help (desc)
  (declare (ccas-desc desc))
  (let* ((mdesc (ccas-desc-mdesc desc))
         (new   (if (eq :undecided (mstatus mdesc))
                    mdesc
                  (ccas-desc-old desc))))
    ;;
    ;; If the ref cell still contains our CCAS desc, then this CAS
    ;; will succeeed.
    ;;
    ;; If not, then it is because another thread has already been
    ;; through here pushing our CCAS desc and succeeded.
    ;;
    (cas (ccas-desc-ref desc) desc new)))

(defun ccas-read (ref)
  (let ((v (ref-value ref)))
    (cond ((ccas-desc-p v)
           (ccas-help v)
           (ccas-read ref))

          (t  v)
          )))

;; ------------------
;; MCAS - Multiple CAS

(defun %mcas (triples)
  ;; triples - a sequence of (ref old new) suitable for CAS
  (mcas-help (make-mcas-desc
              :triples triples
              :status  (list :undecided))))

(defmacro mcas (&rest terms)
  `(%mcas (um:triples ,@terms)))

(defun mcas-help (desc)
  (declare (mcas-desc desc))
  (let ((triples       (mcas-desc-triples desc))
        (status-ref    (mcas-desc-status desc)))
    
    (labels ((decide (desired-state)
               (sys:compare-and-swap (car status-ref) :undecided desired-state)
               (let ((success (eq :successful (car status-ref))))
                 (labels ((patch-up (ref old new)
                            (cas ref desc (if success new old))))
                   (map nil (lambda (triple)
                              (apply #'patch-up triple))
                        triples)
                   (return-from mcas-help success))))

             (acquire (ref old new)
               (multiple-value-bind (t/f v)
                   (ccas ref old desc)
                 (unless t/f
                   (cond ((eq v desc))
                          ;; break
                          )
                         
                         ((mcas-desc-p v)
                          ;; someone else is trying, help them out, then
                          ;; try again
                          (mcas-help v)
                          (acquire ref old new))
                         
                         (t ;; not a descriptor, and not eq old with
                            ;; :undecided, so we must have missed our
                            ;; chance
                            (decide :failed))
                         ))
                 )))

      (when (eq :undecided (car status-ref))
        (map nil (lambda (triple)
                   (apply #'acquire triple))
             triples))
      (decide :successful)
      )))

(defun mcas-read (ref)
  (let ((v (ccas-read ref)))
    (cond ((mcas-desc-p v)
           (mcas-help v)
           (mcas-read ref))

          (t  v)
          )))


#|
(progn
  (defun tstx (&optional (n 1000000))
    (let ((a  (ref 1))
          (b  (ref 2))
          (ct 0))
      (rch:spawn (lambda ()
                   (loop repeat n do
                         (loop until (mcas a 1 3
                                           b 2 4))
                         (incf ct)
                         (mcas a 3 5
                               b 4 6))))
      (loop repeat n do
            (loop until (mcas a 5 7
                              b 6 8))
            (incf ct)
            (mcas a 7 1
                  b 8 2))
      ct))
) ;; progn
|#

  