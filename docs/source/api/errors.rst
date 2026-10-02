:mod:`Pyro5.errors` --- Exception classes
=========================================

The exception hierarchy is as follows::

    Exception
      |
      +-- PyroError
            |
            +-- NamingError
                  |
                  +-- StaleCursorError
            +-- DaemonError
            +-- SecurityError
            +-- CommunicationError
                  |
                  +-- ConnectionClosedError
                  +-- TimeoutError
                  +-- ProtocolError
                          |
                          +-- MessageTooLargeError
                          +-- SerializeError


.. automodule:: Pyro5.errors
   :members:
