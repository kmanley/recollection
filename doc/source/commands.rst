Commands
=================

Recollection allows you to store instances of several TODO:

Data Manipulation
------------------

Recollection is a key-value store and the fundamental operations are put() and get().

.. method:: put(key, value, **kwargs)

  Sets the value for the specified key. The value can be any of the supported :ref:`datatypes`.

  Algorithmic complexity: **O(1)**

  Journal output: <TXID>:<KEYPATH>:**PUT**::<SERIALIZED_VALUE>

  Example::

      put('x', 123)                                     # put an integer
      put('x', 123.45)                                  # put a float
      put('x', 'Hello')                                 # put a string
      put('x', u'Hello')                                # put a Unicode string
      put('x', [1,2,3])                                 # put a list
      put('x', [1,2,3,{'foo':'bar', 'baz':[100,200]}])  # put a complex nested structure


.. method:: put(key, idx0, ...idxN, value, **kwargs)

  Sets a subvalue for the object already stored at key

  Algorithmic complexity: **O(TODO:)**

  Example::

      put('x', [1, 2, 3, [100, 200, 300]])
      put('x', 3, 1, 'Hello')
      # x now contains [1, 2, 3, [100, u'Hello', 300]]




Server Information
~~~~~~~~~~~~~~~~~~~~~~
.. method:: info()

   Returns server information

Example::

      info()
      # TODO: results



