"""
If you use ActiveMQ to run these examples, make sure you enable the STOMP connector, (see `here <http://activemq.apache.org/stomp.html>`_ for details):

.. code-block:: xml
    
    <!-- add this to the config file "activemq.xml" -->
    <transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>
    
For debugging purposes, it is highly recommended to turn on the logger on level :attr:`DEBUG`::

    import logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
"""