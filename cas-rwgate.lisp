;; cas-rwgate.lisp -- R/W Lock done with SMP CAS
;;
;; DM/RAL  02/17
;; -----------------------------------------------------

(defpackage #:cas-rwgate
  (:use #:common-lisp)
  (:import-from #:mcas
   #:ref
   #:car-ref
   #:cdr-ref
   #:mcas-read
   #:mcas)
  (:export
   #:make-rwgate
   #:with-readlock
   #:with-writelock
   ))

(in-package #:cas-rwgate)

(declaim (optimize (speed 3) (safety 0) (float 0)))

;; ---------------------------------------------------------------
;; This package implements an SMP-safe multiple-reader/single-writer
;; lock protocol.
;;
;; Rules of engagement:
;;
;; 1. A lock is available for reading if:
;;      (a) no write locks are in place, or
;;      (b) thread already owns the write lock
;;
;; 2. A lock is available for writing if:
;;      (a) no read locks and no write locks, or
;;      (b) thread already owns the write lock.
;;
;; These rules ensure that multiple readers can run, while only one
;; writer can run. No readers can run while a writer has the lock.
;;
;; A lock holder can request any number of additional locks of the
;; same kind (read/write). The lock will actually be released when an
;; equal number of releases have been performed.
;;
;; If a thread already owns a lock of a given kind, then requesting an
;; additional one imposes zero overhead, and is immediately granted.
;;
;; ---------------------------------------------------------------

(defstruct (rwgate
            (:constructor %make-rwgate))
  hdref
  tlref)

(defun make-rwgate ()
  (let ((cell (ref 0)))
    (%make-rwgate
     ;; pre-allocate read-only refs
     :hdref  (car-ref cell)
     :tlref  (cdr-ref cell))))

(defun rwg-cas (gate locktype)
  (declare (rwgate gate))
  (let* ((new    (case locktype
                  (:read  t)
                  (:write mp:*current-process*)))
         (hdref (rwgate-hdref gate))
         (tlref (rwgate-tlref gate))
         (hd    (mcas-read hdref))
         (tl    (mcas-read tlref)))
    (declare (fixnum hd))
    (cond ((mcas hdref hd            (1+ hd)
                 tlref (and tl new)  new)
           ;; we got it
           )
          
          ((or (null tl)
               (eq new tl))
           ;; try again - we might have just failed on the count
           (rwg-cas gate locktype))
          )))

(defun rwg-release (gate)
  (declare (rwgate gate))
  (let* ((hdref  (rwgate-hdref gate))
         (tlref  (rwgate-tlref gate))
         (hd     (mcas-read hdref))
         (tl     (mcas-read tlref))
         (new    (and (> hd 1)
                      tl)))
    (declare (fixnum hd))
    (unless (mcas hdref  hd  (1- hd)
                  tlref  tl  new)
      (rwg-release gate))
    ))

;; ------------------------------------------------------

(Defmacro safe-unwind-protect (form &rest unwind-clauses)
  `(mp:with-interrupts-blocked
    (unwind-protect
        (progn
          (mp:current-process-unblock-interrupts)
          ,form)
      ,@unwind-clauses)))

#+:LISPWORKS
(editor:setup-indent "safe-unwind-protect" 1)

;; ------------------------------------------------------

(defun do-with-lock (have-lock gate locktype fn timeout abortfn)
  (if have-lock
      (funcall fn)
    ;; else
    (labels ((ok-to-proceed ()
               (rwg-cas gate locktype)))
      
      (if (or (ok-to-proceed)
              (mp:wait-processing-events timeout
                                         :wait-function #'ok-to-proceed))
          (safe-unwind-protect
              (funcall fn)
            (rwg-release gate))
        ;; else
        (when abortfn
          (funcall abortfn))
        ))))

;; --------------------------------------------------

(defvar *have-read-lock*  nil)
(defvar *have-write-lock* nil)

(defmacro with-read-lock ((rw-lock &key timeout abortfn) &body body)
  `(do-with-lock (or *have-read-lock*
                     *have-write-lock*)
                 ,rw-lock
                 :read
                 (lambda ()
                   (let ((*have-read-lock* t))
                     ,@body))
                 ,timeout
                 ,abortfn))

(defmacro with-write-lock ((rw-lock &key timeout abortfn) &body body)
  `(do-with-lock *have-write-lock*
                 ,rw-lock
                 :write
                 (lambda ()
                   (let ((*have-write-lock* t))
                     ,@body))
                 ,timeout
                 ,abortfn))
