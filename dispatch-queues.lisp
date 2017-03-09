;; dispatch-queues.lisp
;;
;; DM/RAL 02/17
;; -----------------------------------------------------------------
;;
;; Serial and Parallel Dispatch
;;
;; Groups are defined implicitly on dispatch queues, as well as by
;; user group names. Waiting on a dispatch group allows for selective
;; waits when numerous, possibly unrelated, groups of tasks are
;; launched in parallel.
;;
;; Waiting on a queue, as contrasted with waiting on a group, implies
;; waiting until all tasks on the queue have been finalized,
;; regardless of group assignments.
;;
;; Dispatch can be Serial or Parallel, Synchronous or Asynchronous.
;; All four combinations are defined: DISPATCH-SERIAL-SYNC,
;; DISPATCH-SERIAL-ASYNC, DISPATCH-PARALLEL-SYNC, and
;; DISPATCH-PARALLEL-ASYNC.  
;;
;; Serial dispatch implies a serialized ordering of tasks on a queue,
;; but operate in parallel with the caller thread. Serial dispatch
;; queues operate in parallel with each other and the (implicit)
;; parallel dispatch queue. Serial dispatch queues can be created by
;; the user to segregate groups of tasks, but NIL refers to the
;; default serial dispatch queue, created on demand.
;;
;; All parallel dispatches, as opposed to serial dispatches, are
;; conceptually grouped as one default multi-headed queue, and imply
;; no serialized ordering to the dispatched tasks.
;;
;; Synchronous dispatch implies a wait for results, so parallelism
;; with the caller ceases until the task completes. The task can be
;; launched on either a serial dispatch queue, which implies a
;; serialized ordering among tasks in the queue, and the reply will
;; occur only after the launched synchronous task has its chance to
;; run to completion after arriving at the head of the queue. Or else
;; a task can be launched with parallel dispatch, which implies
;; immediate and arbitrary ordering of the task, with the response
;; arriving whenever the task has its chance to run.
;;
;; Asynchronous dispatch fires off a task in parallel, and any results
;; from the task are discarded. Dispatch can occur on a serial queue,
;; or on the (implicit) parallel dispatch queue. The same remarks
;; about task ordering apply here.
;;
;; Final synchronization among launched tasks can be had with
;; WAIT-FOR-SERIAL-DISPATCH, WAIT-FOR-PARALLEL-DISPATCH, and
;; WAIT-FOR-DISPATCH-GROUP. The first two functions accept an optional
;; group or queue argument.
;;
;; For parallel dispatch, no argument in WAIT-FOR-PARALLEL-DISPATCH
;; implies a wait for all parallel dispatched activity to complete. A
;; group argument implies a wait for those tasks which were dispatched
;; with that group assignment, regardless of how they were dispatched.
;;
;; For serial dispatch, no argument in WAIT-FOR-SERIAL-DISPATCH
;; implies a wait on all tasks assigned to the default serial dispatch
;; queue. A queue argument implies a wait on all tasks assigned to
;; that queue, and a group name argument implies a wait on all tasks
;; dispatched in that group, regardless of how they were dispatched.
;;
;; Tasks may be launched, for any given group ID, into any combination
;; of serial and parallel dispatches. When a specific group can be specified,
;; use the WAIT-FOR-DISPATCH-GROUP and the group ID as argument.
;;
;; Group ID's can be anything that compares EQ. We suggest a keyword symbol.

(in-package um.dispq)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (import 'um:defmonitor))

(declaim (hcl:special-global *default-serial-dispatch-queue*
                             *dispatch-groups*))

;; --------------------------------------------------------
;; utilities...

(defstruct capture-packet
  data)

(defun capture-ans-or-exn (fn &rest args)
  (make-capture-packet
   :data (multiple-value-list
          (ignore-errors
            (multiple-value-list (apply fn args))))
   ))

(defmethod recover-ans-or-exn ((capt capture-packet))
  (multiple-value-bind (ans exn)
      (values-list (capture-packet-data capt))
    (if exn
        (error exn)
      (values-list ans))))

;; -------------
;; Dispatch Timeouts

(defvar *dispatch-timeout* nil)

(defmacro with-dispatch-timeout (timeout &body body)
  `(let ((*dispatch-timeout* ,timeout))
     (declare (special *dispatch-timeout*))
     ,@body))

#+:LISPWORKS
(editor:setup-indent "WITH-DISPATCH-TIMEOUT" 2)

;; -----------------------------------------------------------
;; Dispatch Groups

(defstruct dispatch-group
  (lock  (mp:make-lock))
  (cxv   (mp:make-condition-variable))
  (count 0))

(defvar *dispatch-groups* (make-hash-table
                           :weak-kind :value))

(defun %wrap-dispatch-group (grp fn &rest args)
  ;; this provides more specific granularity. But failure modes might
  ;; include:
  ;;   1. the resulting lambda is never dispatched - bad use
  ;;      symptoms - wait-for-dispatch-group will hang waiting forever
  ;;      solution - use Reppy events with timeouts
  ;;
  ;; The alternative, to wait until thread is launched before
  ;; incrementing the group count, means that:
  ;;   1. the system could possibly exit wait-for-dispatch-group before it
  ;;      has fully processed all the potential threads in the group, or
  ;;   2. a failure to launch the thread would leave work that never runs
  ;;      and the program might be unaware.
  ;;
  ;; We mark this as intended only for internal use by prefixing the
  ;; name with "%"
  ;;
  (let ((cell (hcl:gethash-ensuring grp *dispatch-groups* #'make-dispatch-group)))
    (with-accessors ((lock  dispatch-group-lock)
                     (cxv   dispatch-group-cxv)
                     (count dispatch-group-count)) cell
      (mp:with-lock (lock)
        (incf count)
        (lambda ()
          (unwind-protect
              (apply fn args)
            (mp:with-lock (lock)
              (when (zerop (decf count))
                (mp:condition-variable-broadcast cxv)))
            ))
        ))))
  
(defun wait-for-dispatch-group (grp &key (timeout *dispatch-timeout*))
  ;; return T on successful sync
  ;; NIL on timeout
  (let ((cell (gethash grp *dispatch-groups*))
        (ok   t))
    (assert cell)
    (with-accessors ((lock  dispatch-group-lock)
                     (cxv   dispatch-group-cxv)
                     (count dispatch-group-count)) cell
      (mp:with-lock (lock)
        (loop until (zerop count)
              do
              (unless (mp:condition-variable-wait cxv lock :timeout timeout)
                ;; returns nil on timeout
                (setf ok (zerop count))
                (loop-finish))
              ))
      ok)))
  
(defmacro %with-dispatch-group (grp &body body)
  ;; To avoid the possible problems mentioned in WRAP-DISPATCH-GROUP
  ;; please avoid calling this or WRAP-DISPATCH-GROUP directly in your
  ;; program code.  Instead, use WITH-SERIAL-DISPATCH,
  ;; WITH-PARALLEL-DISPATCH, WITH-PRIORITY-DISPATCH
  `(%wrap-dispatch-group ,grp (lambda ()
                               ,@body)))

;; -------------------
;; standardized dispatch handlers

(defun %do-serial-dispatch-loop (get-item-fn &rest args)
  (loop
   (optima:ematch (apply get-item-fn args)
     
     ((list (list* fn args) reply-mb)
      ;; Capture results or errors and send back to caller
      (mp:mailbox-send reply-mb (apply #'capture-ans-or-exn fn args)))
     
     ((list (list* fn args))
      ;; Let another thread handle any errors. Caller doesn't care
      ;; about errors, but we want the user to become aware of them
      ;; without blowing the server thread away.
      (let ((sem  (mp:make-semaphore :count 0)))
        ;; synchronous execution keeps us serializing tasks
        (mp:funcall-async (lambda ()
                            (unwind-protect
                                (apply fn args)
                              (mp:semaphore-release sem))))
        (mp:semaphore-acquire sem)))
     )))
   
;; --------------------------------------------------------
;; Serialized Dispatch Servers - A FIFO task handler
;; (see data-objects/priority-queue for a prioritized task handler)

(defstruct (serial-dispatch-queue
            (:constructor %make-serial-dispatch-queue))
  mgr-t mbox)

(defun make-serial-dispatch-queue (&key name
                                        (mbox (mp:make-mailbox)))
  (labels ((serve ()
             (%do-serial-dispatch-loop #'mp:process-wait-for-event)))
    (%make-serial-dispatch-queue
     :mbox   mbox
     :mgr-t  (mp:process-run-function (or name
                                          (gensym "Serial-Dispatch-Queue-"))
                                      `(:mailbox ,mbox
                                        :internal-server t)
                                      #'serve))
    ))

;; -------------
;; Default Serial Dispatch Queue -- used when caller indicates a null queue argument

(defvar *default-serial-dispatch-queue* nil)
  
(defmonitor
  (ensure-default-dispatch-queue ()
     (let ((qname "Default-Serial-Dispatch-Queue"))
       (with-accessors ((mgr-t  serial-dispatch-queue-mgr-t)
                        (mbox   serial-dispatch-queue-mbox)) *default-serial-dispatch-queue*
         (or
          ;; if we already have an extant server, just return the queue
          (and *default-serial-dispatch-queue*
               (mp:process-alive-p mgr-t)
               *default-serial-dispatch-queue*)
          
          ;; we had a server, now dead. So create a new server, keeping
          ;; the existing mailbox so we don't lose any enqueued tasks
          (and *default-serial-dispatch-queue*
               (setf *default-serial-dispatch-queue*
                     (make-serial-dispatch-queue
                      :mbox mbox
                      :name qname)))
          
          ;; we never had a server, so instantiate one
          (setf *default-serial-dispatch-queue*
                (make-serial-dispatch-queue
                 :name qname))
          )))))

;; -------------
;; Serialized Dispatch on a Serial Queue

(defun %do-serial-dispatch-async (dispatch-fn fn &rest args)
  (funcall dispatch-fn `((,fn ,@args)) ))

(defun %do-serial-dispatch-sync (dispatch-fn fn &rest args)
  (let ((reply-mb (mp:make-mailbox)))
    (funcall dispatch-fn `((,fn ,@args) ,reply-mb))
    (recover-ans-or-exn (mp:mailbox-read reply-mb :timeout *dispatch-timeout*))))
  
(defun %do-serial-dispatch (dispatch-fn q fn &rest args)
  ;; null q denotes default dispatch-queue
  (let ((q (or q (ensure-default-dispatch-queue))))
    (funcall dispatch-fn
             (um:rcurry (um:curry #'mp:process-send (serial-dispatch-queue-mgr-t q))
                        :error-if-dead-p t)
             (apply #'%wrap-dispatch-group q fn args))
    ))

(defun dispatch-serial-sync (q fn &rest args)
  (apply #'%do-serial-dispatch #'%do-serial-dispatch-sync q fn args))

(defun dispatch-serial-async (q fn &rest args)
  (apply #'%do-serial-dispatch #'%do-serial-dispatch-async q fn args))

(defun wait-for-serial-dispatch (&key (group (ensure-default-dispatch-queue))
                                      (timeout *dispatch-timeout*))
  ;; group defaults to the default serial dispatch queue. It can also
  ;; be any other serial dispatch queue or a group ID.
  (wait-for-dispatch-group group
                           :timeout timeout))

(defmacro with-serial-dispatch ((&key queue
                                      group
                                      (async t)
                                      (timeout *dispatch-timeout*))
                                &body body)
  ;; defaults to sync
  (let ((dispatch (if async
                      'dispatch-serial-async
                    'dispatch-serial-sync))
        (fn  (if group
                 `(%with-dispatch-group ,group
                    ,@body)
               `(lambda ()
                  ,@body))))
    `(with-dispatch-timeout ,timeout
         (,dispatch ,queue ,fn))
    ))

;; ----------------------------------------------------------------
;; Parallel/Concurrent Dispatch

(defvar +par-unique+ (um:make-unique-object))

(defun %do-parallel-dispatch-sync (fn)
  (let ((reply-mb (mp:make-mailbox)))
    (mp:funcall-async (lambda ()
                        (mp:mailbox-send reply-mb
                                         (capture-ans-or-exn fn))))
    (recover-ans-or-exn (mp:mailbox-read reply-mb :timeout *dispatch-timeout*))))

(defun %do-parallel-dispatch (dispatch-fn fn &rest args)
  (funcall dispatch-fn (apply #'%wrap-dispatch-group +par-unique+ fn args)))

(defun dispatch-parallel-sync (fn &rest args)
  (apply #'%do-parallel-dispatch #'%do-parallel-dispatch-sync fn args))
  
(defun dispatch-parallel-async (fn &rest args)
  (apply #'%do-parallel-dispatch #'mp:funcall-async fn args))

(defun wait-for-parallel-dispatch (&key (group +par-unique+)
                                        (timeout *dispatch-timeout*))
  ;; group defaults to the implicit parallel dispatch queue, but could
  ;; also be any group ID
  (wait-for-dispatch-group group
                           :timeout timeout))

(defmacro with-parallel-dispatch ((&key group
                                        (async t)
                                        (timeout *dispatch-timeout*))
                                  &body body)
  ;; defaults to async
  (let ((dispatch (if async
                      'dispatch-parallel-async
                    'dispatch-parallel-sync))
        (fn (if group
                `(%with-dispatch-group ,group
                   ,@body)
              `(lambda ()
                 ,@body))))
    `(with-dispatch-timeout ,timeout
         (,dispatch ,fn))
    ))

;; -------------------------------------------
;; PAR Construct

(defun nsplit-list (n lst)
  ;; destructively split a list into an n element front list
  ;; and the remainder in a back list.
  (if (plusp n)
      (values lst
              (um:when-let (tail (nthcdr (1- n) lst))
                (shiftf (cdr tail) nil)))
    ;; else
    (values nil lst)))

(defun %par (&rest fns)
  ;; Accept a list of functions, execute them all in parallel.
  ;;
  ;; Since they are all slated for parallel execution, evaluation
  ;; order must be irrelevant.
  ;;
  ;; We group them to try to balance the load among available threads,
  ;; including our own. Since our thread is doing all the work, try to
  ;; keep the shortest subgroup for our thread.
  ;;
  (let* ((sem    (mp:make-semaphore :count 0))
         (nfns   (length fns))
         (nthr   (1+ (mp:set-funcall-async-limit nil)))
         (ngrp   (ceiling nfns nthr))
         (nrem   (rem nfns ngrp))
         (nshort (if (zerop nrem) ngrp nrem)))
    (multiple-value-bind (short-list rest-list)
        (nsplit-list nshort fns)
      (map nil (lambda (fn)
                 (mp:funcall-async (lambda ()
                                     (unwind-protect
                                         (funcall fn)
                                       (mp:semaphore-release sem)))
                                   ))
           rest-list)
      (map nil #'funcall short-list)
      (mp:semaphore-acquire sem :count (- nfns nshort))
      )))

(defmacro par (&rest clauses)
  ;; Accept a list of clauses and execute them in parallel,
  ;; synchronizing at the closing paren.
  (if (rest clauses)
      `(%par ,@(mapcar #`(lambda ()
                           ,a1)
                       clauses))
    ;; else
    (first clauses)))

#|
  ;; NOTE: if you execute this from the editor, the editor will use
  ;; one BG thread for itself.  And so the results will be off,
  ;; compared to executing this from the listener or some other non-BG
  ;; thread.
  ;;
  ;; Since we had up to 5 BG threads, this ought to take 3 seconds for
  ;; 18 clauses.  Adding just one more clause copy will push us to 4
  ;; seconds.
(time
 (par
   (sleep 1) ;; 1
   (sleep 1) ;; 2
   (sleep 1) ;; 3
   (sleep 1) ;; 4
   (sleep 1) ;; 5
   
   (sleep 1) ;; 1
   (sleep 1) ;; 2
   (sleep 1) ;; 3
   (sleep 1) ;; 4
   (sleep 1) ;; 5

   (sleep 1) ;; 1
   (sleep 1) ;; 2
   (sleep 1) ;; 3
   (sleep 1) ;; 4
   (sleep 1) ;; 5
   
   (sleep 1) ;; 1
   (sleep 1) ;; 2
   (sleep 1) ;; 3
   ))
 |#
