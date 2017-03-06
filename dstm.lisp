;; dstm.lisp -- Dynamic STM - Software Transactional Memory
;;
;; See paper: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.59.8787&rep=rep1&type=pdf
;;  "Software Transactional Memory for Dynamic-Sized Data Structures",
;;  Herlihy, Luchangco, Moir, Sherer
;;
;; DM/RAL  02/17
;; --------------------------------------------------------

#|
(defpackage #:dstm
  (:use #:common-lisp)
  (:import-from #:ref
   #:ref
   #:mref
   #:ref-value
   #:cas)
  (:export
   #:make-var
   #:atomic
   #:open-for-read
   #:open-for-write
   #:release-read
   #:abort-transaction
   #:rollback
   #:clone
   ))
|#

(in-package #:dstm)

;; ----------------------------------------------------

(defvar *current-transaction* nil)

(defstruct transaction
  ro-table
  dly-table
  (state (ref :active)))

(defvar *ncomms*  0)  ;; cumm nbr successful commits
(defvar *nrolls*  0)  ;; cumm nbr rollbacks

;; ----------------------------------------------------

(define-condition rollback-exn ()
  ())

(define-condition abort-exn ()
  ((arg  :reader abort-exn-retval :initarg :retval :initform nil)))

(defun dstm-error (exn)
  (cas (transaction-state *current-transaction*) :active :aborted)
  (error exn))

(defun rollback ()
  (sys:atomic-fixnum-incf *nrolls*)
  (dstm-error (load-time-value
               (make-condition 'rollback-exn)
               t)))

(defun abort-transaction (&optional retval)
  (if retval
      (dstm-error (make-condition 'abort-exn
                                  :retval retval))
    ;; else
    (dstm-error (load-time-value
                 (make-condition 'abort-exn)
                 t))))

;; ----------------------------------------------------

(defvar *permanently-committed*
  (make-transaction
   :state (ref :committed)))

(defclass locator ()
  ((new   :accessor val-new        :initarg :new   :initform nil)
   (old   :accessor val-old        :initarg :old   :initform nil)
   (trans :accessor val-trans      :initarg :trans :initform *permanently-committed*)
   ))

(defun make-var (&optional val)
  (ref (make-instance 'locator
                      :new  val)))

;; ----------------------------------------------------

(defun should-abort (var)
  (let* ((entry (or (find var (transaction-dly-table *current-transaction*)
                          :key  #'first
                          :test #'eq)
                    (let ((ent (list var 0.001)))
                      (push ent (transaction-dly-table *current-transaction*))
                      ent)
                    ))
         (dly  (second entry)))
    (if (< dly 0.1)
        (progn
          ;; be nice...
          (sleep (random dly))
          (setf (second entry) (* dly 1.618))
          nil)
      ;; else
      (progn
        ;; no more Mr. Nice Guy...
        (sleep (random dly))
        t))
    ))

(defun add-to-read-only-table (var val trans)
  (um:if-let (entry (find var (transaction-ro-table *current-transaction*)
                          :key  #'first
                          :test #'eq))
      (progn
        (unless (eq val (second entry))
          (rollback))
        (unless (eq :write (third entry))
          (incf (the fixnum (third entry)))))
    ;; else
    (let ((ct  (if (eq trans *current-transaction*)
                   :write
                 1)))
      (push (list var val ct) (transaction-ro-table *current-transaction*)))
    ))

(defun get-committed-val (var)
  (let* ((loc   (ref-value var))
         (trans (val-trans loc)))
    (case (ref-value (transaction-state trans))
      (:committed
       (setf (val-old loc) nil) ;; help GC
       (values (val-new loc) loc))
      
      (:aborted
       (setf (val-new loc) nil) ;; help GC
       (values (val-old loc) loc))
      
      (:active     (cond ((eq trans *current-transaction*)
                          (values (val-old loc) loc))

                         ((should-abort var)
                          ;; abort him and try again
                          (cas (transaction-state trans) :active :aborted)
                          (get-committed-val var))

                         (t
                          ;; try again
                          (get-committed-val var))
                         ))
      )))

(defun validate-read-only-table ()
  ;; check that all vars opened for reading remain unchanged
  (unless (every #'(lambda (triple)
                     (eq (second triple)
                         (get-committed-val (first triple))))
                 (transaction-ro-table *current-transaction*))
    (rollback)))

(defun validate-transaction ()
  ;; check vars opened for reading and that we are still active
  (validate-read-only-table)
  (unless (eq :active (ref-value (transaction-state *current-transaction*)))
    (rollback)))

(defun open-for-read (var)
  (multiple-value-bind (val loc)
      (get-committed-val var)
    (add-to-read-only-table var val (val-trans loc))
    (validate-transaction)
    val))

(defun release-read (var)
  ;; releasing a var that was never opened is a benign error
  (um:when-let (entry (find var (transaction-ro-table *current-transaction*)
                            :key  #'first
                            :test #'eq))
    (unless (or (eq :write (third entry))
                (plusp (the fixnum (decf (the fixnum (third entry))))))
      (setf (transaction-ro-table *current-transaction*)
            (delete entry (transaction-ro-table *current-transaction*)
                    :test #'eq)))
    ))
      
(defun update-read-only-table-for-write (var val)
  ;; if var has been opened for reading, mark it as now open for writing
  ;; prevents release-read from doing anything
  (um:when-let (entry (find var (transaction-ro-table *current-transaction*)
                            :key  #'first
                            :test #'eq))
    (progn
      (unless (eq val (second entry))
        (rollback))
      (setf (third entry) :write))
    ))

;; ------------------------------
;; All objects subject to DSTM must define a DSTM:CLONE method
;;
;; CLONE is called when opening for write access.
;;
;; NOTE: These are all shallow (skeleton spine) copies Be careful not
;; to mutate any of the contained objects, but mutation of spine cells
;; is fine.  When operating on a copy, the use of single-thread
;; algorithms is fine. No other threads can see it.

(defmethod clone ((lst list))
  (copy-list lst))

(defmethod clone ((seq sequence))
  (copy-seq seq))

(defmethod clone ((s structure-class))
  (copy-structure s))

(defmethod clone ((r mref))
  (mref (ref-value r)))

;; ------------------------------

(defun open-for-write (var)
  (multiple-value-bind (old loc)
      (get-committed-val var)
    (cond ((eq (val-trans loc) *current-transaction*)
           ;; already open-for-write by us
           (val-new loc))

          (t
           ;; try to grab with new locator
           (let* ((new     (clone old))
                  (new-loc (make-instance 'locator
                                          :new   new
                                          :old   old
                                          :trans *current-transaction*)))
             (cond ((cas var loc new-loc)
                    ;; we got it
                    (update-read-only-table-for-write var old)
                    (validate-transaction)
                    new)
            
                   (t
                    ;; try again
                    (open-for-write var))
                   )))
          )))

(defun commit ()
  ;; check that no read-only vars have changed and that we are still active
  (validate-read-only-table)
  (unless (cas (transaction-state *current-transaction*) :active :committed)
    (rollback))
  ;; we made it
  (sys:atomic-fixnum-incf *ncomms*))

(defun do-atomically (fn)
  (cond (*current-transaction*
         (funcall fn))
        
        (t
         (handler-case
             (loop
              (let ((*current-transaction* (make-transaction)))
                (handler-case
                    (unwind-protect
                        (return-from do-atomically
                          (multiple-value-prog1
                              (values (funcall fn) t)
                            (commit)))
                      ;; unwind - help GC
                      (setf (transaction-ro-table *current-transaction*) nil
                            (transaction-dly-table *current-transaction*) nil))
                  
                  (rollback-exn (exn)
                    (declare (ignore exn))
                    ;; try again from the top
                    )
                  )))

           (abort-exn (exn)
             (values (abort-exn-retval exn) nil))
           ))
        ))
  
(defmacro atomic (&body body)
  ;; return (values body t) if successful
  ;; else (values nil nil) if aborted
  `(do-atomically (lambda ()
                    ,@body)))
       
;; -------------------------------------------------

;; ---------------------------------------------------
;; Test it out... hopefully lots of contention... yep!
#|
(progn
  (defun show-rolls (&optional (duration 1))
    (let ((pcnt (/ *nrolls* *ncomms* 0.01))
          (rate (/ *ncomms* duration)))
      (list :rollbacks *nrolls*
            :commits   *ncomms*
            :percent-rollbacks pcnt
            :commits-per-roll (if (zerop pcnt) :infinite (* 100 (/ pcnt)))
            :duration duration
            :commits-per-sec  rate)))
  
  (defun reset ()
    (setf *nrolls* 0)
    (setf *ncomms* 0))
  
  (defvar *a* (make-var (list 0)))
  (defvar *b* (make-var (list 0)))
  
  (defun check-invariant (&aux a b)
    (atomic
      (setf a (car (open-for-read *a*))
            b (car (open-for-read *b*))
            ))
    (unless (= b (* 2 a))
      (bfly:log-info :SYSTEM-LOG "Invariant broken: A = ~A, B = ~A" a b)))
  
  (defun common-code (delta)
    (atomic
      (let* ((lsta (open-for-write *a*))
             (lstb (open-for-write *b*))
             (a    (+ delta (car lsta)))
             (b    (* 2 a)))
        (setf (car lsta) a
              (car lstb) b)
        )))

  (defvar *ct* 1000)
  
  (defun count-up ()
    (loop repeat *ct* do (common-code 1)))
  
  (defun count-down ()
    (loop repeat *ct* do (common-code -1)))
  
  (defun checker (&rest procs)
    (let ((start (usec:get-time-usec)))
      (loop while (some #'mp:process-alive-p procs)
            do (check-invariant))
      (let ((stop (usec:get-time-usec)))
        (bfly:log-info :SYSTEM-LOG (show-rolls (* 1e-6 (- stop start))))) ))
  
  (defun tst1 ()
    (bfly:log-info :SYSTEM-LOG "Start HDSTM Test...")
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (bfly:spawn #'checker
                :name :checker
                :args (mapcar #'bfly:pid-proc
                              (list (bfly:spawn #'count-down
                                                :name :up-counter)
                                    (bfly:spawn #'count-up
                                                :name :down-counter))))
    )
  
  (defun tst2 (&optional (ct 100000))
    (setf *ct* ct)
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (let ((start (usec:get-time-usec))
          (ct 0)
          (down (bfly:spawn-link #'count-down
                                 :name :down-counter))
          (up   (bfly:spawn-link #'count-up
                                 :name :up-counter)))
      (loop until (= 2 ct)
            do
            (bfly:recv msg
              ((list :Exit-Message pid _ _)
               :when (or (eq pid down)
                         (eq pid up))
               (incf ct))
              ( _ )))
      
      (let ((stop (usec:get-time-usec)))
        (show-rolls (* 1e-6 (- stop start))))
      ))

  (defun tst3 (&optional (ct 100000))
    ;; only one thread for no-contention timings
    (setf *ct* ct)
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (let ((start (usec:get-time-usec))
          (ct 0)
          (down (bfly:spawn-link #'count-down
                                 :name :down-counter)))
      (loop until (= 1 ct)
            do
            (bfly:recv msg
              ((list :exit-message pid _ _)
               :when (eq pid down)
               (incf ct))
              ( _ )))
      
      (let ((stop (usec:get-time-usec)))
        (show-rolls (* 1e-6 (- stop start))))
      ))
  
  ;; -------------------------------------------

  (defun tst4 ()
    ;; only one thread for no-contention timings
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (let ((start (usec:get-time-usec))
          (ct 0)
          (down (bfly:spawn-link #'count-down
                                 :name :down-counter)))
      (loop until (= 1 ct)
            do
            (bfly:recv msg
              ((list* :exit-message pid _)
               :when (eq pid down)
               (incf ct))
              ( _ )))
      
      (let ((stop (usec:get-time-usec)))
        (/ (- stop start) 5e6))))
  
  ) ;; progn
|#


