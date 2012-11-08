Introduction
============

.. rubric :: stomp, stomper, stompest!


`stompest <https://github.com/mozes/stompest/>`_ is a full-featured implementation of the `STOMP <http://stomp.github.com/>`_ protocol (versions `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_) for Python 2.6 (and higher) including both synchronous and `Twisted <http://twistedmatrix.com/>`_ clients:

* The :class:`sync.Stomp` client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.
* The :class:`async.Stomp` client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect, receipt, and disconnect timeouts.

Both clients make use of a generic set of components in the :mod:`protocol` module each of which can be used independently to roll your own STOMP client:

* a wire-level STOMP frame parser :class:`protocol.StompParser` and compiler :class:`protocol.StompFrame`,

* a faithful implementation of the syntax of the STOMP 1.0 and 1.1 protocols with a simple stateless function API in the :mod:`protocol.commands` module,

* a generic implementation of the STOMP 1.0 and 1.1 session state semantics in :class:`protocol.StompSession`, such as protocol version negotiation at connect time, transaction and subscription handling (including a generic subscription replay scheme which may be used to reconstruct the session's subscription state after a forced disconnect),

* and :class:`protocol.StompFailoverProtocol`, a `failover transport <http://activemq.apache.org/failover-transport-reference.html>`_ URI scheme akin to the one used in ActiveMQ.

This module is thoroughly unit tested and production hardened for the functionality used by `Mozes <http://www.mozes.com/>`_ --- persistent queueing on `ActiveMQ <http://activemq.apache.org/>`_. Other users also use it in serious production environments.

Contents
========

.. toctree::
   :maxdepth: 2
   
   sync
   async
   protocol
   error
