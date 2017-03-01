;; ref.lisp -- SMP indirect refs
;;
;; DM/RAL  02/17
;; -------------------------------------------------------------

(defpackage #:ref
  (:use #:common-lisp)
  (:export
   #:ref
   #:car-ref
   #:cdr-ref
   #:ref-value
   #:cas
   #:atomic-incf
   #:atomic-decf
   ))

(in-package #:ref)
   
(declaim (optimize (speed 3) (safety 0) (float 0)))

;; ====================================================================

(defclass ref-mixin ()
  ((cell :reader ref-cell :initarg :cell)))

;; ----------------------------------------

(defclass car-ref (ref-mixin)
  ())

(defmethod ref-value ((ref car-ref))
  (car (ref-cell ref)))

(defmethod cas ((ref car-ref) old new)
  (system:compare-and-swap (car (ref-cell ref)) old new))

(defmethod atomic-incf ((ref car-ref))
  (system:atomic-fixnum-incf (car (ref-cell ref))))

(defmethod atomic-decf ((ref car-ref))
  (system:atomic-fixnum-decf (car (ref-cell ref))))

;; -----------------------------------------

(defclass ref (car-ref)
  ())

(defun ref (x)
  (make-instance 'ref
                 :cell (list x)))

(defmethod car-ref ((ref ref))
  ref)

;; -----------------------------------------

(defclass cdr-ref (ref-mixin)
  ())

(defmethod cdr-ref ((ref ref))
  (make-instance 'cdr-ref
                 :cell (ref-cell ref)))

(defmethod ref-value ((ref cdr-ref))
  (cdr (ref-cell ref)))

(defmethod cas ((ref cdr-ref) old new)
  (system:compare-and-swap (cdr (ref-cell ref)) old new))

(defmethod atomic-incf ((ref cdr-ref))
  (system:atomic-fixnum-incf (cdr (ref-cell ref))))

(defmethod atomic-decf ((ref cdr-ref))
  (system:atomic-fixnum-decf (cdr (ref-cell ref))))

