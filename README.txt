stomp, stomper, stompest!
=========================

`stompest <https://github.com/nikipore/stompest/>`_ is a full-featured implementation of the `STOMP <http://stomp.github.com/>`_ protocol (versions `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_) for Python 2.6 (and higher) including both synchronous and asynchronous clients:

* The synchronous client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.
* The asynchronous client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect, receipt, and disconnect timeouts.

Both clients make use of a generic set of components in the each of which can be used independently to roll your own STOMP client:

* a wire-level STOMP frame parser and compiler,

* a faithful implementation of the syntax of the STOMP 1.0 and 1.1 protocols with a simple stateless function API,

* a generic implementation of the STOMP 1.0 and 1.1 session state semantics, such as protocol version negotiation at connect time, transaction and subscription handling (including a generic subscription replay scheme which may be used to reconstruct the session's subscription state after a forced disconnect),

* and a `failover transport <http://activemq.apache.org/failover-transport-reference.html>`_ URI scheme akin to the one used in ActiveMQ.

stompest 2 is fully documented and probably even better tested than stompest 1: it is about to be used in production by one of the authors. In the thorough redesign, stompest has gained heavily on functionality, and the authors valued consistency, simplicity and symmetry over full backward compatibility to stompest 1. It is planned to add more features in the near future. Thus, the API should not be considered stable, which is why stompest 2 is still marked as (mature) alpha software.
