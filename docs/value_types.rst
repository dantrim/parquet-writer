.. _sec:value_types:

Storing Basic Value Types
=========================

``parquet-writer`` currently has support for storing boolean and numeric
data types. 

The following table describes the supported value types for data
to be written to an output Parquet file, with the ``parquet-writer`` name
that would be used in the JSON file providing the layout declaration
for ``parquet-writer``.

.. _tab:value_types:

+--------------------------+------------------------------------------------+
| Value Types              | ``parquet-writer`` name                        |
+==========================+================================================+
| Boolean                  | ``bool``                                       |
+--------------------------+------------------------------------------------+
| Signed Integers          | ``int8``, ``int16``, ``int32``, ``int64``      |
+--------------------------+------------------------------------------------+
| Unsigned Integers        | ``uint8``, ``uint16``, ``uint32``, ``uint64``  |
+--------------------------+------------------------------------------------+
| Floating Point           | ``float`` (32-bit precision),                  |
|                          | ``double`` (64-bit precision)                  |
+--------------------------+------------------------------------------------+

In addition to writing flat data columns of these basic value types,
``parquet-writer`` supports writing data columns that are
nested data structures composed of fields whose data is comprised
of these basic value types.
More specifically, ``parquet-writer`` supports:

 * 1, 2, and 3 dimensional lists of these value types
 * Struct data types having any number of named fields (like a C/C++ ``struct``)
 * 1, 2, and 3 dimensional lists of struct data type

More information on how to declare and write Parquet files containing
these nested structures is contained in later sections.

Declaring Columns of Basic Value Types
--------------------------------------

Declaring columns storing values of the basic data types above
in JSON, one constructs a JSON layout as follows:

.. _codeblock:basiclayout:

.. code-block:: json

    {
      "fields": [
        {"name": "column0", "type": "float"},
        {"name": "column1", "type": "int32"}
      ]
    }

That is, one must specify a ``fields`` array containing JSON objects
of the form:

.. code-block:: json

  {"name": "<string>", "type": "<value-type>"}
  
where the ``name`` field
can be any arbitrary string. The ``type`` field must be one of the
``parquet-writer`` names for the supported basic value types appearing in the
second column in the :ref:`table above<tab:value_types>`.

Each element in the top-level ``fields`` array in a given JSON
layout configuration will have a one-to-one correspondence with a data column
appearing in the output Parquet file.

Writing Columns of Basic Value Types
------------------------------------

Assuming we have the :ref:`file layout from above<codeblock:basiclayout>`,
one would simply need to have variables of the corresponding C++
type to provide to the ``Writer`` class' ``fill`` function, along with
the name of the column to which you want to write the data:

.. code-block:: cpp

    #include "parquet_writer.h"
    ...
    float field0_data = 42.5;
    int32_t field1_data = 42;
    ...
    writer.fill("column0", field0_data);
    writer.fill("column1", field1_data);
    ...
    writer.end_row();

Note that the order in which the columns are filled is not important.
One could also do the filling in this order:

.. code-block:: cpp

    writer.fill("column1", field1_data);
    writer.fill("column0", field0_data);

