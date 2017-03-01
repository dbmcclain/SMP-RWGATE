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
   #:mcas
   #:mcas1
   #:mcas-read
   ))

(in-package #:mcas)
   
(declaim (optimize (speed 3) (safety 0) (float 0)))

;; ------------------
;; CCAS - Conditional CAS

(defstruct ccas-desc
  ref old new cond)

(defun ccas (ref old new cond)
  (labels ((try-ccas (desc)
             (declare (ccas-desc desc))
             (cond ((cas ref old desc)
                    (ccas-help desc))
                   (t  (let ((v (ref-value ref)))
                         (when (ccas-desc-p v)
                           (ccas-help v)
                           (try-ccas desc))))
                   )))
    (try-ccas (make-ccas-desc
               :ref      ref
               :old      old
               :new      new
               :cond     cond))
    ))

(defun ccas-help (desc)
  (declare (ccas-desc desc))
  (let ((new  (if (eq :undecided (ref-value (ccas-desc-cond desc)))
                  (ccas-desc-new desc)
                (ccas-desc-old desc))))
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

(defstruct mcas-desc
  triples status)

(defun mcas (triples)
  ;; triples - a sequence of (ref old new) suitable for CAS
  (mcas-help (make-mcas-desc
              :triples triples
              :status  (ref :undecided))))

(defun mcas1 (ref old new)
  (mcas `((,ref ,old ,new))))

(defun mcas-help (desc)
  (declare (mcas-desc desc))
  (let ((triples    (mcas-desc-triples desc))
        (status-ref (mcas-desc-status desc)))
    
    (labels ((decide (desired)
               (cas status-ref :undecided desired)
               (let ((success (eq :successful (ref-value status-ref))))
                 (map nil (lambda (triple)
                            (apply #'patch-up success triple))
                      triples)
                 success))

             (patch-up (success ref old new)
               (cas ref desc (if success new old)))

             (try-mcas (ref old new)
               (ccas ref old desc status-ref)
               (let ((v (ccas-read ref))) ;; ccas-read makes it faster than when using ref-value (!??)
                 (cond ((and (eq v old)
                             (eq :undecided (ref-value status-ref)))
                        ;; must have changed beneath us when we
                        ;; looked, then changed back again. So try
                        ;; again. (ABA Update?)
                        (try-mcas ref old new))
                       
                       ((eq v desc)
                        ;; we got this one, try the next candidate
                       :break)
                       
                       ((mcas-desc-p v)
                        ;; someone else is trying, help them out, then
                        ;; try again
                        (mcas-help v)
                        (try-mcas ref old new))
                       
                       (t ;; not a descriptor, and not eq old with
                          ;; :undecided, so we must have missed our
                          ;; chance
                        (return-from mcas-help (decide :failed)))
                       ))))

      (map nil (lambda (triple)
                 (apply #'try-mcas triple))
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

