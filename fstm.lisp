;; fstm.lisp -- Dynamic STM - Software Transactional Memory
;;
;; Adapted from UCAM-CL-TR-579 U.Cambridge Tech Report 579,
;; "Practical lock-freedom" by Keir Fraser, Feb 2004
;;
;; See also paper: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.59.8787&rep=rep1&type=pdf
;;  "Software Transactional Memory for Dynamic-Sized Data Structures",
;;  Herlihy, Luchangco, Moir, Sherer
;;
;; DM/RAL  03/17
;; --------------------------------------------------------

#|
(defpackage #:fstm
  (:use #:common-lisp)
  (:import-from #:ref
   #:ref
   #:mref
   #:ref-value
   #:cas
   #:atomic-incf)
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

(in-package #:fstm)

;; ----------------------------------------------------

(defvar *current-transaction* nil)

(defvar *tid-counter*  0)

(defun next-tid ()
  (sys:atomic-fixnum-incf *tid-counter*))

(defvar *vid-counter*  0)

(defun next-vid ()
  (sys:atomic-fixnum-incf *vid-counter*))

(defstruct transaction
  (id      (next-tid))
  (state   (list :undecided))
  ro-list
  (rw-map  (maps:empty)))

(defvar *ncomms*  0)  ;; cumm nbr successful commits
(defvar *nrolls*  0)  ;; cumm nbr retrys

;; ----------------------------------------------------

(define-condition retry-exn ()
  ())

(define-condition abort-exn ()
  ((arg  :reader abort-exn-retval :initarg :retval :initform nil)))

(defun retry ()
  (sys:atomic-fixnum-incf *nrolls*)
  (error (load-time-value
          (make-condition 'retry-exn)
          t)))

(defun abort-transaction (&optional retval)
  (if retval
      (error (make-condition 'abort-exn
                             :retval retval))
    ;; else
    (error (load-time-value
            (make-condition 'abort-exn)
            t))))

;; ----------------------------------------------------

(defstruct (var
            (:constructor %make-var))
  (id  (next-vid))
  ref)

(defun make-var (&optional val)
  (%make-var
   :ref  (list val)))

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

;; ----------------------------------------------------

(declaim (inline trans< tstate))

(defun trans< (trans1 trans2)
  (declare (type transaction trans1 trans2))
  (< (the fixnum (transaction-id trans1))
     (the fixnum (transaction-id trans2))))

(defun tstate (trans)
  (declare (type transaction trans))
  (car (transaction-state trans)))

;; -------------------------------------------------------------------

;; Here, it is known that other-trans is in the midst of a commit.  If
;; it is still in the reading / grapping stage of commit, then we look
;; to see if we are also committing. If we are, we wouldn't be here
;; unless we had already grabbed all of our write cells. There is a
;; possibility that other-trans may also want some of those same write
;; cells, and the outcome of its commit is uncertain. To avoid an
;; infinite loop, if we are older than other-tran, then we abort other
;; tran.

;; Otherwise, whether we are committing or not, we help him commit, so
;; that we gain a definitive answer to his commit outcome. If he wants
;; some of our write cells, then

;; If it has proceeded past the point of grabbing all its write cells
;; and is now in :READ-PHASE, then we help it along.
;;
;; If trans (which is us) is also in a commit, and if we are older
;; than other-trans, then we attempt to abort other-trans. That will
;; either succeed, or else other-trans has already finished
;; committing. Either way it will have a status of :FAILED or
;; :SUCCEEDED.
;;
;; (NOTE: if we are in a commit, and in this routine, then we must
;; already be in :READ-PHASE)
;;
;; If we weren't in a commit, then we merely help other-trans finish
;; its commit.

(defun maybe-help (trans other-trans)
  (when (eq :read-phase (tstate other-trans))
    ;; other-trans is in the :read-phase of a commit
    (cond ((and (eq :read-phase (tstate trans)) ;; are we in :read-phase of commit too?
                (trans< trans other-trans))     ;; and are we older?
           ;; we are older - abort him
           (sys:compare-and-swap (car (transaction-state other-trans)) :read-phase :failed))

          (t ;; either we are not in a commit, or we are younger
           (commit-transaction other-trans))
          )))

;; --------------------------------------

(defun obj-read(trans var)
  (let ((data (car (var-ref var))))
    (when (transaction-p data)
      (let ((entry (maps:find (var-id var) (transaction-rw-map data))))
        (maybe-help trans data)
        (setf data (if (eq :successful (tstate data))
                       (third entry)  ;; new
                     (second entry))) ;; old
        ))
    data))

;; --------------------------------------

(defun commit-transaction (trans)
  (let ((state-ref     (transaction-state trans))
        (rw-map        (transaction-rw-map trans))
        (desired-state :failed))
    
    (labels ((get-status ()
               (let ((state (car state-ref)))
                 (case state
                   (:failed     nil)
                   (:successful t)
                   (t
                    (sys:compare-and-swap (car state-ref) state desired-state)
                    (get-status))
                   )))
             
             (decide ()
               (let ((success (get-status)))
                 (labels ((restore (var old new)
                            (sys:compare-and-swap (car (var-ref var)) trans (if success new old))))
                   (maps:iter #'(lambda (k v)
                                  (declare (ignore k))
                                  (apply #'restore v))
                              rw-map)
                   (return-from commit-transaction success))))

             (acquire (var old new)
               (unless (sys:compare-and-swap (car (var-ref var)) old trans)
                 (let ((data (car (var-ref var))))
                   (cond ((eq data trans)
                          ;; break
                          )
                         
                         ((transaction-p data)
                          ;; someone else is committing, help them out
                          (commit-transaction data)
                          (acquire var old new))
                         
                         (t
                          ;; didn't get it, can't get it
                          (decide))
                         )))))

      (when (eq :undecided (car state-ref))
        (maps:iter #'(lambda (k v)
                       (declare (ignore k))
                       (apply #'acquire v))
                   rw-map)
        (sys:compare-and-swap (car state-ref) :undecided :read-phase))
      (when (eq :read-phase (car state-ref))
        (loop for (var val) in (transaction-ro-list trans) do
              (unless (eq val (obj-read trans var))
                (decide)))
        (setf desired-state :successful))
      (decide)
      )))

;; -------------------------------------------------------------------------
;; operations on *CURRENT-TRANSACTION*

(defun open-for-read (var)
  (let ((data (obj-read *current-transaction* var)))
    (push (list var data) (transaction-ro-list *current-transaction*))
    data))

(defun open-for-write (var)
  (let ((triple (maps:find (var-id var) (transaction-rw-map *current-transaction*))))
    (cond (triple
           (third triple)) ;; current new value
          
          (t
           (let* ((pair  (find var (transaction-ro-list *current-transaction*)
                               :key  #'first
                               :test #'eq))
                  (old (cond (pair
                              (setf (transaction-ro-list *current-transaction*)
                                    (delete var (transaction-ro-list *current-transaction*)
                                            :key  #'first
                                            :test #'eq))
                              (second pair)) ;; current val
                   
                             (t
                              (obj-read *current-transaction* var))
                             ))
                  (new  (clone old)))
             (setf (transaction-rw-map *current-transaction*)
                   (maps:add (var-id var) (list var old new)
                             (transaction-rw-map *current-transaction*)))
             new))
          )))

(defun release-read (var)
  ;; releasing a var that was never opened is a benign error
  (setf (transaction-ro-list *current-transaction*)
        (delete var (transaction-ro-list *current-transaction*)
                :key   #'first
                :test  #'eq
                :count 1)))
      
(defun commit ()
  (unless (commit-transaction *current-transaction*)
    (retry))
  (sys:atomic-fixnum-incf *ncomms*))

;; ------------------------------

(defun do-atomically (fn)
  (cond (*current-transaction*
         (funcall fn))
        
        (t
         (handler-case
             (loop
              (let ((*current-transaction* (make-transaction)))
                (handler-case
                    (return-from do-atomically
                      (multiple-value-prog1
                          (values (funcall fn) t)
                        (commit)))
                  
                  (retry-exn (exn)
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

#|
;; Speed Comparison DSTM/FSTM (median of 3 runs)
;;
;; Duration Measurements (1M Iters)
Test      DSTM       FSTM
----      ----       ----
TST1      2.23       4.57
TST2      4.50       9.59
TST3      6.72      14.82
|#


