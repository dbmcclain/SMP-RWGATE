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
;; QCAS -- (Query CAS) A CAS with a returned value of the actual
;; contents of a ref before CAS success/failure

(defun QCAS (ref old new)
  (let (qold)
    (hcl:unwind-protect-blocking-interrupts-in-cleanups
        (progn
          (loop until (cas ref
                           (setf qold (ref-value ref))
                           mp:*current-process*))
          (cond ((eq qold old)
                 (setf qold new)
                 (values t old))
                
                (t
                 (values nil qold))))
      (cas ref mp:*current-process* qold))))

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
                 (qcas ref old desc)
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
                   (cond ((or (eq v old)
                              (eq v desc))
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

      (map nil (lambda (triple)
                 (apply #'acquire triple))
           triples)
      (decide :successful)
      )))
          
(defun mcas-read (ref)
  (let ((v (ccas-read ref)))
    (cond ((mcas-desc-p v)
           (mcas-help v)
           (mcas-read ref))

          (t  v)
          )))

