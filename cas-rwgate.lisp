;; cas-rwgate.lisp -- R/W Lock done with SMP CAS
;;
;; DM/RAL  02/17
;; -----------------------------------------------------

(defpackage #:cas-rwgate
  (:use #:common-lisp)
  (:export
   #:make-rwgate
   #:with-readlock
   #:with-writelock))

(in-package #:cas-rwgate)

;; ---------------------------------------------------------------
;; This package implements a multiple-reader/single-writer lock
;; protocol using the amazing capabilities of the Reppy channels.
;;
;; Rule of engagement:
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

(defun make-rwgate ()
  (cons nil 0))

(defun rwg-cas (gate locktype)
  (let ((new    (case locktype
                  (:read  t)
                  (:write mp:*current-process*))))
    (and (sys:compare-and-swap (car gate)
                               (and (car gate)
                                    new)
                               new)
         (sys:atomic-fixnum-incf (cdr gate)))
    ))

(defun rwg-release (gate)
  (when (zerop (sys:atomic-fixnum-decf (cdr gate)))
    (setf (car gate) nil)))

;; ------------------------------------------------------

(defmacro safe-unwind-protect (form &rest unwind-clauses)
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
