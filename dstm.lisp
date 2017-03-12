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
   #:set-ref-value
   #:cas)
  (:export
   #:make-var
   #:atomic
   #:open-for-read
   #:open-for-write
   #:release-read
   #:abort-transaction
   #:retry
   #:clone
   ))
|#

(in-package #:dstm)

(declaim (inline cas-state
                 locator-new locator-old locator-trans
                 ro-ent-var ro-ent-val ro-ent-refct
                 transaction-ro-table transaction-state)
         (optimize (speed 3) (safety 0) (float 0)))

;; ----------------------------------------------------

(defvar *current-transaction* nil)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defstruct transaction
    ro-table
    dly-table
    (state  :active)))

(defmethod cas ((trans transaction) old new)
  (sys:compare-and-swap (transaction-state trans) old new))

;; ----------------------------------------------------

(defvar *ncomms*  0)  ;; cumm nbr successful commits
(defvar *nrolls*  0)  ;; cumm nbr retrys

;; ----------------------------------------------------

(define-condition retry-exn ()
  ())

(define-condition abort-exn ()
  ((arg  :reader abort-exn-retval :initarg :retval :initform nil)))

(defun dstm-error (exn)
  (cas *current-transaction* :active :aborted)
  (error exn))

(defun retry ()
  (sys:atomic-fixnum-incf *nrolls*)
  (dstm-error (load-time-value
               (make-condition 'retry-exn)
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
   :state :committed))

(defstruct locator
  new old
  (trans  *permanently-committed*))

;; ----------------------------------------------------

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defstruct (var
              (:constructor %make-var))
    loc))

(defun make-var (&optional val)
  (%make-var
   :loc (make-locator
         :new val)))

(defmethod cas ((var var) old-loc new-loc)
  (system:compare-and-swap (var-loc var) old-loc new-loc))

#|
(defun make-var (&optional val)
  (ref (make-locator
        :new  val)))
|#

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

(defmethod clone ((s structure-object))
  (copy-structure s))

(defmethod clone ((r mref))
  (mref (ref-value r)))

(defmethod clone ((v var))
  ;; Not a good idea to clone a var
  ;; Should always incorporate vars in some structured object
  ;; but never as the value of another var.
  (error "Attempt to clone a Var"))

;; ----------------------------------------------------

(defstruct dly-ent
  var dly)

(defun should-abort (var)
  (with-accessors ((dly-table  transaction-dly-table))
      *current-transaction*
    (let* ((entry (or (find var dly-table
                            :key  #'dly-ent-var
                            :test #'eq)
                      (let ((ent (make-dly-ent
                                  :var var
                                  :dly 0.001)))
                        (push ent dly-table)
                        ent)
                      ))
           (dly  (dly-ent-dly entry))
           (rdly (random dly)))
      (cond ((< dly 0.1)
             ;; be nice...
             (mp:wait-processing-events rdly)
             (setf (dly-ent-dly entry) (* dly 1.618))
             nil) ;; say no to aborting...
            
            (t 
             ;; no more Mr. Nice Guy...
             (mp:wait-processing-events rdly)
             t) ;; go ahead and abort him
            ))))

;; -------------------------------------------------------------------------
;; entry tuples in transaction ro-list

(defstruct ro-ent
  var val refct)

(defun add-to-read-only-table (var val trans)
  (with-accessors ((ro-table  transaction-ro-table))
      *current-transaction*
    (unless (eq trans *current-transaction*)
      (let ((entry  (find var ro-table
                          :key  #'ro-ent-var
                          :test #'eq)))
        (cond (entry
               ;; don't worry here about val v.s. our copy of it.
               ;; we'll be checking consistency momentarily.
               (incf (the fixnum (ro-ent-refct entry))))

              (t 
               (push (make-ro-ent
                      :var   var
                      :val   val
                      :refct 1)
                     ro-table))
              )))))

(defun update-read-only-table-for-write (var)
  ;; if var has been opened for reading, mark it as now open for writing
  ;; prevents release-read from doing anything
  (with-accessors ((ro-table  transaction-ro-table))
      *current-transaction*
    (setf ro-table (delete var ro-table
                           :key  #'ro-ent-var
                           :test #'eq))
    ))

(defun validate-read-only-table ()
  ;; check that all vars opened for reading remain unchanged
  (unless (every #'(lambda (entry)
                     (eq (ro-ent-val entry)
                         (get-committed-val (ro-ent-var entry))))
                 (transaction-ro-table *current-transaction*))
    (retry)))

;; -----------------------------------------------------------------

(defun get-committed-val (var)
  (let* ((loc   (var-loc var))
         (trans (locator-trans loc)))
    (case (transaction-state trans)
      (:committed
       (setf (locator-old loc) nil) ;; help GC
       (values (locator-new loc) loc))
      
      (:aborted
       (setf (locator-new loc) nil) ;; help GC
       (values (locator-old loc) loc))
      
      (:active
       (cond ((eq trans *current-transaction*)
              (values (locator-old loc) loc))
             
             ((should-abort var)
              ;; abort him and try again
              (cas trans :active :aborted)
              (get-committed-val var))
             
             (t
              ;; try again
              (get-committed-val var))
             ))
      )))

(defun validate-transaction ()
  ;; check vars opened for reading and that we are still active
  (validate-read-only-table)
  (unless (eq :active (transaction-state *current-transaction*))
    (retry)))

(defun open-for-read (var)
  (multiple-value-bind (val loc)
      (get-committed-val var)
    (add-to-read-only-table var val (locator-trans loc))
    (validate-transaction)
    val))

(defun release-read (var)
  ;; releasing a var that was never opened is a benign error
  (with-accessors ((ro-table  transaction-ro-table))
      *current-transaction*
  (um:when-let (entry (find var ro-table
                            :key  #'ro-ent-var
                            :test #'eq))
    (unless (plusp (the fixnum (decf (the fixnum (ro-ent-refct entry)))))
      (setf ro-table (delete entry ro-table
                             :test  #'eq
                             :count 1)))
    )))
      
;; ------------------------------

(defclass wref ()
  ((loc  :reader wref-loc  :initarg :loc)))

(defun wref (loc)
  (make-instance 'wref
                 :loc loc))

(defmethod ref-value ((w wref))
  (locator-new (wref-loc w)))

(defmethod set-ref-value ((w wref) val)
  (setf (locator-new (wref-loc w)) val))

(defun open-for-write (var)
  (multiple-value-bind (old loc)
      (get-committed-val var)
    (cond ((eq (locator-trans loc) *current-transaction*)
           ;; already open-for-write by us
           (wref loc))
          
          (t
           ;; try to grab with new locator
           (let* ((new     (clone old))
                  (new-loc (make-locator
                            :new   new
                            :old   old
                            :trans *current-transaction*)))
             (cond ((cas var loc new-loc)
                    ;; we got it
                    (update-read-only-table-for-write var)
                    (validate-transaction)
                    (wref new-loc))
                   
                   (t
                    ;; try again
                    (open-for-write var))
                   )))
          )))

;; ------------------------------

(defun commit ()
  ;; check that no read-only vars have changed and that we are still active
  (validate-read-only-table)
  (unless (cas *current-transaction* :active :committed)
    (retry))
  ;; we made it
  (sys:atomic-fixnum-incf *ncomms*))

;; ------------------------------

(defun do-atomic (fn)
  (cond (*current-transaction*
         (funcall fn))
        
        (t
         (labels ((try-transaction ()
                    (with-accessors ((ro-table  transaction-ro-table)
                                     (dly-table transaction-dly-table))
                        *current-transaction*
                      
                      (unwind-protect
                          (return-from do-atomic
                            (multiple-value-prog1
                                (values (funcall fn) t)
                              (commit)))
                        
                        ;; unwind - help GC
                        (setf ro-table  nil
                              dly-table nil))
                      ))


                  (retry-loop ()
                    (let ((*current-transaction* (make-transaction)))
                      (handler-case
                          (try-transaction)
                        (retry-exn (exn)
                          (declare (ignore exn))
                          ;; try again from the top
                          (retry-loop))
                        ))
                    ))
                    
           (handler-case
               (retry-loop)
             (abort-exn (exn)
               (values (abort-exn-retval exn) nil))
             )
           ))
        ))
  
(defmacro atomic (&body body)
  ;; return (values body t) if successful
  ;; else (values nil nil) if aborted
  `(do-atomic (lambda ()
                ,@body)))
       
;; -------------------------------------------------

;; ---------------------------------------------------
;; Test it out... hopefully lots of contention... yep!
#|
(progn
  (defun show-rolls (&optional (duration 1))
    (let ((pcnt (/ *nrolls* *ncomms* 0.01))
          (rate (/ *ncomms* duration)))
      (list :retrys *nrolls*
            :commits   *ncomms*
            :percent-retrys pcnt
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
    (if (= b (* 2 a))
        (format t "~%a = ~A, b = ~A  (~A)" a b mp:*current-process*)
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
    (loop repeat *ct* do (common-code 1))
    (check-invariant)
    )
  
  (defun count-down ()
    (loop repeat *ct* do (common-code -1))
    (check-invariant)
    )
  
  (defun checker (&rest procs)
    (let ((start (usec:get-time-usec)))
      (loop while (some #'mp:process-alive-p procs)
            do (check-invariant))
      (let ((stop (usec:get-time-usec)))
        (bfly:log-info :SYSTEM-LOG (show-rolls (* 1e-6 (- stop start))))) ))
  
  (defun tst0 ()
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
  
  (defun tst1 (&optional (ct 100000))
    ;; only one thread for no-contention timings
    (setf *ct* ct)
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (let ((start (usec:get-time-usec)))
      (count-down)
      (let ((stop (usec:get-time-usec)))
        (show-rolls (* 1e-6 (- stop start))))
      ))
  
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
    (setf *ct* ct)
    (setf *a* (make-var (list 0))
          *b* (make-var (list 0)))
    (reset)
    (let ((start (usec:get-time-usec))
          (ct 0)
          (down (bfly:spawn-link #'count-down
                                 :name :down-counter))
          (up   (bfly:spawn-link #'count-up
                                 :name :up-counter))
          (down2 (bfly:spawn-link #'count-down
                                  :name :down-counter2)))
      (loop until (= 3 ct)
            do
            (bfly:recv msg
              ((list :Exit-Message pid _ _)
               :when (or (eq pid down)
                         (eq pid up)
                         (eq pid down2))
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
