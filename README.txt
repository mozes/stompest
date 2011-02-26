stomp, stomper, stompest!

Stompest is a no-nonsense Stomp implementation for Python including both synchronous and Twisted clients.

Modeled after the Perl Net::Stomp module, the synchronous client is dead simple.  It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

The Twisted client is a full-featured STOMP protocol client built on top of the stomper library.  It supports destination-specific message handlers, concurrent message processing, graceful shutdown, "poison pill" error handling, and connection timeout.

This module is thoroughly unit tested and production hardened for the functionality used by Mozes: persistent queueing on ActiveMQ.  Minor enhancements may be required to use this STOMP adapter with other brokers or certain feature sets like transactions or binary messages.

See examples directory for sample usage.
