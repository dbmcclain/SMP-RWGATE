
This folder started out as an SMP-safe implementation of multiple-CAS
and an application of it for read-many/single-writer gates. It has
been extended with other SMP-safe utilities, such as Dynamic Software
Transactional Memory (DSTM), a more efficient RW-gate done with LW's
own sharing locks, SMP-safe LAZY, ONCE-ONLY, FUTURE, FORCE, PMAP,
PVMAP, and a Lisp-based implementation of serial and parallel dispatch
queues for parallel processing.

Enjoy!

- DM/RAL  03/17
