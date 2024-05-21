# Nessie Client directly accessing a Nessie backend

... without REST or any other intermediate protocol.

**NOT FOR PRODUCTION USE**

* No access checks
* (possibly) not fully tested and (possibly) does not provide the same behavior
* Meant for testing use cases that do not want to or cannot use (another) Jersey/Weld instance
* (Currently?) only implemented for (and useful with?) an in-memory backend
