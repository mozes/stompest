stomp, stomper, stompest!
=========================

`stompest <https://github.com/mozes/stompest/>`_ is a full-featured implementation of the `STOMP <http://stomp.github.com/>`_ protocol (versions `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_) for Python 2.6 (and higher) including both synchronous and `Twisted <http://twistedmatrix.com/>`_ clients:

* The synchronous :class:`~.stompest.sync.client.Stomp` client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.
* The asynchronous :class:`~.stompest.async.client.Stomp` client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect, receipt, and disconnect timeouts.

Both clients make use of a generic set of components in the :mod:`~.stompest.protocol` module each of which can be used independently to roll your own STOMP client:

* a wire-level STOMP frame parser :class:`~.stompest.protocol.parser.StompParser` and compiler :class:`~.stompest.protocol.frame.StompFrame`,

* a faithful implementation of the syntax of the STOMP 1.0 and 1.1 protocols with a simple stateless function API in the :mod:`~.stompest.protocol.commands` module,

* a generic implementation of the STOMP 1.0 and 1.1 session state semantics in :class:`~.stompest.protocol.session.StompSession`, such as protocol version negotiation at connect time, transaction and subscription handling (including a generic subscription replay scheme which may be used to reconstruct the session's subscription state after a forced disconnect),

* and :class:`~.stompest.protocol.failover.StompFailoverProtocol`, a `failover transport <http://activemq.apache.org/failover-transport-reference.html>`_ URI scheme akin to the one used in ActiveMQ.

This module is thoroughly unit tested and production hardened for the functionality used by `Mozes <http://www.mozes.com/>`_ --- persistent queueing on `ActiveMQ <http://activemq.apache.org/>`_. Others also deploy stompest in serious production environments. The substantial redesigned stompest 2 is probably even better tested but should be considered (mature) alpha: Some features to come (in particular heartbeating and STOMP 1.2) may still require minor changes of the API.

A few words about history: When stompest was brought into being, there were already several STOMP related modules around, but none of them did address the main requirements: a simple access to the STOMP protocol which stays out of your way (the synchronous :class:`~.stompest.sync.client.Stomp` client) features and an event-driven implementation which leverages the power of the `Twisted <http://twistedmatrix.com/>`_ framework (the asynchronous :class:`~.stompest.async.client.Stomp` client). The `stomper <http://code.google.com/p/stomper/>`_ package provided an abstract implementation of the STOMP protocol which stompest 1 took advantage of: stomp. stomper, stompest! During the redesign which lead to stompest 2, the `stomper <http://code.google.com/p/stomper/>`_ package turned out to suffer from being out-of-maintenance, so the appealing feature of an abstract STOMP API was insourced. Kudos to Oisin Mulvihill, the developer of `stomper <http://code.google.com/p/stomper/>`_!

Contents
========

.. toctree::
    :maxdepth: 2
	
    sync
    async
    config
    protocol
    error
