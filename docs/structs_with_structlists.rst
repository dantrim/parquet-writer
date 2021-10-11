.. _sec:structs_with_structlists:

Storing Structs That Have Struct List Fields
============================================

``parquet-writer`` allows for storing ``struct`` typed columns
that have named fields that are lists
of ``struct`` typed elements.

Declaring Structs That Have Struct List Fields
----------------------------------------------

Specifying a ``struct`` type column with a field that is a list
of ``struct`` type elements is done as follows:

.. code-block:: json

    {
      "fields": [
        {"name": "my_struct", "type": "struct",
         "fields": [
           {"name": "field0", "type": "float"},
           {"name": "structlist", "type": "list1d",
            "contains": {
                         "type": "struct",
                         "fields": [
                           {"name": "foo", "type": "float"},
                           {"name": "bar", "type": "int32"}
                         ]
                        }
           }
         ]
       }
      ]
    }

The above describes a Parquet file containing a ``struct`` typed
column named ``my_struct`` with two named fields: ``field0`` that is 
of type ``float`` and ``structlist`` that is a one-dimensional list
of ``struct`` type elements each having two fields named ``foo``
and ``bar``.

The above pattern of course works also for two- and three-dimensional
lists of ``struct`` type elements simply by swapping out
the ``list1d`` type for ``list2d`` or ``list3d``, as needed.

.. note::
    The :ref:`struct list constraints<subsec:struct_list_constraints>` still
    hold for the case when the struct list is associated with a
    named field of a ``struct`` typed column.
