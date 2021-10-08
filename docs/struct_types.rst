.. _sec:struct_types:

Storing Struct Type Columns
===========================

Storing complex data structures with any number of named fields of possibly
different data types (think: C/C++ ``struct``) is possible in
``parquet-writer``.

Declaring Struct Type Columns
-----------------------------

Declaring columns containing struct typed data is done via
the ``struct`` type specifier.
For example, a struct typed column with three named fields
``field0``, ``field1``, and ``field2`` with
data types ``int32``, ``float``, and ``list1d[float]``, respectively,
is done as follows:

.. code-block:: json

    {
      "fields": [
        {
          "name": "struct_column", "type": "struct",
          "fields": [
            {"name": "field0", "type": "int32"},
            {"name": "field1", "type": "float"},
            {"name": "field2", "type": "list1d", "contains": {"type": "float"}}
          ]
        }
      ]
    }

As can be seen, ``struct`` types are declared with an additional ``fields`` array
which contains an array of objects of the usual ``{"name": ..., "type": ...}`` form
which describes each of the named fields of the output data structure
to be stored in the Parquet file.

Writing Struct Type Columns
---------------------------

There are two convenience types that are used for writing data to
``struct`` typed columns:

  1. ``parquetwriter::field_map_t``
  2. ``parquetwriter::field_buffer_t``

The ``field_map_t`` type is an alias for ``std::vector<std::string, parquetwriter::value_t>``,
where ``parquetwriter::value_t`` refers to an instance of any of the :ref:`basic value types<sec:value_types>`.


