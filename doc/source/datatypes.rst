Data types
=================

Recollection allows you to store instances of sever

List
------
.. index: list
Recollection allows you to store lists in the key-value store. These lists are subclasses of the Python builtin
list class, and support most of the same operations.

.. method:: list()

  Creates a new empty list

  Algorithmic complexity: **O(1)**

  Example::

      put("x", list())

      put("x", [])

.. method:: list(iterable)

  Creates a new list initialized from iterable's items

  Algorithmic complexity: **O(n)**

  Example::

      put("x", x for x in range(100) if x % 2 ==0)
      # x now contains [0, 2, 4, ...98]


.. method:: list.__add__(other_list)

  Returns the result of list.append(other_list). Does not modify either list or other_list

  Algorithmic complexity: **O(n)** where n is size of other_list

  Example::

      put("x", [1, 2, 3])
      get("x") + [10, 20, 30] # -> returns [1, 2, 3, 10, 20, 30]


Unupported operations
~~~~~~~~~~~~~~~~~~~~~~
.. method:: list.__delslice__(i, j)

  Deletes slice from list[i:j].
  This is not supported


 |
 |  Methods defined here:
 |
 |  __add__(...)
 |      x.__add__(y) <==> x+y
 |
 |  __contains__(...)
 |      x.__contains__(y) <==> y in x
 |
 |  __delitem__(...)
 |      x.__delitem__(y) <==> del x[y]
 |
 |  __delslice__(...)
 |      x.__delslice__(i, j) <==> del x[i:j]
 |
 |      Use of negative indices is not supported.
 |
 |  __eq__(...)
 |      x.__eq__(y) <==> x==y
 |
 |  __ge__(...)
 |      x.__ge__(y) <==> x>=y
 |
 |  __getattribute__(...)
 |      x.__getattribute__('name') <==> x.name
 |
 |  __getitem__(...)
 |      x.__getitem__(y) <==> x[y]
 |
 |  __getslice__(...)
 |      x.__getslice__(i, j) <==> x[i:j]
 |
 |      Use of negative indices is not supported.
 |
 |  __gt__(...)
 |      x.__gt__(y) <==> x>y
 |
 |  __iadd__(...)
 |      x.__iadd__(y) <==> x+=y
 |
 |  __imul__(...)
 |      x.__imul__(y) <==> x*=y
 |
 |  __init__(...)
 |      x.__init__(...) initializes x; see help(type(x)) for signature
 |
 |  __iter__(...)
 |      x.__iter__() <==> iter(x)
 |
 |  __le__(...)
 |      x.__le__(y) <==> x<=y
 |
 |  __len__(...)
 |      x.__len__() <==> len(x)
 |
 |  __lt__(...)
 |      x.__lt__(y) <==> x<y
 |
 |  __mul__(...)
 |      x.__mul__(n) <==> x*n
 |
 |  __ne__(...)
 |      x.__ne__(y) <==> x!=y
 |
 |  __repr__(...)
 |      x.__repr__() <==> repr(x)
 |
 |  __reversed__(...)
 |      L.__reversed__() -- return a reverse iterator over the list
 |
 |  __rmul__(...)
 |      x.__rmul__(n) <==> n*x
 |
 |  __setitem__(...)
 |      x.__setitem__(i, y) <==> x[i]=y
 |
 |  __setslice__(...)
 |      x.__setslice__(i, j, y) <==> x[i:j]=y
 |
 |      Use  of negative indices is not supported.
 |
 |  __sizeof__(...)
 |      L.__sizeof__() -- size of L in memory, in bytes
 |
 |  append(...)
 |      L.append(object) -- append object to end
 |
 |  count(...)
 |      L.count(value) -> integer -- return number of occurrences of value
 |
 |  extend(...)
 |      L.extend(iterable) -- extend list by appending elements from the iterable
 |
 |  index(...)
 |      L.index(value, [start, [stop]]) -> integer -- return first index of value.
 |      Raises ValueError if the value is not present.
 |
 |  insert(...)
 |      L.insert(index, object) -- insert object before index
 |
 |  pop(...)
 |      L.pop([index]) -> item -- remove and return item at index (default last).
 |      Raises IndexError if list is empty or index is out of range.
 |
 |  remove(...)
 |      L.remove(value) -- remove first occurrence of value.
 |      Raises ValueError if the value is not present.
 |
 |  reverse(...)
 |      L.reverse() -- reverse *IN PLACE*
 |
 |  sort(...)
 |      L.sort(cmp=None, key=None, reverse=False) -- stable sort *IN PLACE*;
 |      cmp(x, y) -> -1, 0, 1

.. method:: list.insert(index, object)

  Inserts *object* before *index*

  Algorithmic complexity: **O(n)**

  Example::

      put("x", [1,2,3])
      get("x").insert(0, "hi")
      # x now contains ["hi", 1, 2, 3]


extend
+++++++++

append
+++++++++


Set
------



Dict
------




.. code-block:: python

 def my_function():
     "just a test"
     print 8/2

+------------------------+------------+----------+----------+
| Header row, column 1   | Header 2   | Header 3 | Header 4 |
| (header rows optional) |            |          |          |
+========================+============+==========+==========+
| body row 1, column 1   | column 2   | column 3 | column 4 |
+------------------------+------------+----------+----------+
| body row 2             | Cells may span columns.          |
+------------------------+------------+---------------------+
| body row 3             | Cells may  | - Table cells       |
+------------------------+ span rows. | - contain           |
|    lambda x: x * 3     |            | - body elements.    |
|                        |            |                     |
+------------------------+------------+---------------------+


.. table:: Truth table for "not"

   =====  =====
     A    not A
   =====  =====
   False  True
   True   False
   =====  =====