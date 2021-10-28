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

For example, a struct-typed column with three named fields
``field0``, ``field1``, and ``field2`` with
data types ``int32``, ``float``, and ``list1d[float]``, respectively,
is done as follows:

.. _layout:struct_example:

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

As can be seen, columns of type ``struct`` are declared with an additional ``fields`` array
that contains an array of objects of the usual ``{"name": ..., "type": ...}`` form.
The additional ``fields`` array describes each of the named fields of the data structure
to be stored in the output Parquet file.

Writing Struct Type Columns
---------------------------

There are two convenience types that are used for writing data to
columns with type ``struct``:

  1. ``parquetwriter::field_map_t``
  2. ``parquetwriter::field_buffer_t``

The ``field_map_t`` type is an alias for ``std::map<std::string, parquetwriter::value_t>``,
where ``parquetwriter::value_t`` refers to an instance of any of the :ref:`basic value types<sec:value_types>`.
The ``field_map_t`` type allows users to fill ``struct`` type columns
without worrying aabout the order of the struct's fields as declared
in the JSON layout.

The ``field_buffer_t`` type is an alias for ``std::vector<parquetwriter::value_t>``.

.. warning::
    When using the ``field_buffer_t`` type to 
    write to struct type columns, the user must provide each of the struct's field data
    in the order that the named fields appear in the JSON layout for the struct.

.. _sec:struct_field_map:

Using ``field_map_t``
^^^^^^^^^^^^^^^^^^^^^

An example of filling the three-field struct ``my_struct`` declared in the
:ref:`previous section<layout:struct_example>` would be  as follows:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // generate the data for each of the struct's fields
    int32_t field0_data{42};
    float field1_data{42.42};
    std::vector<float> field2_data{42.0, 42.1, 42.2};

    // create the mapping between column name and data value to be stored
    pw::field_map_t my_struct_data{
        {"field0", field0_data},
        {"field1", field1_data},
        {"field2", field2_data}
    };

    // call "fill" as usual
    writer.fill("my_struct", my_struct_data);

Note that since the ``field_map_t`` convenience type is an alias of ``std::map``,
the ordering of the column names (the keys of the ``std::map``)
does not matter. The following instantiation of the ``field_map_t`` 
would lead to the same output written to file as the above:

.. code-block:: cpp

    pw::field_map_t my_struct_data{
        {"field2", field2_data},
        {"field1", field1_data},
        {"field0", field0_data}
    };

.. note::
    When using the ``field_map_t`` approach to write
    to a struct type column, the call to ``fill`` leads to an
    internal check against the loaded layout for the specific struct-type column
    and constructs an intermediate ``field_buffer_t`` with the data values
    in the order matching that of the loaded layout.

.. _sec:struct_field_buffer:

Using ``field_buffer_t``
^^^^^^^^^^^^^^^^^^^^^^^^

The alternative approach using ``field_buffer_t`` to write the struct 
``my_struct`` from :ref:`above<layout:struct_example>` would be as follows:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // generate the data for each of the struct's fields
    int32_t field0_data{42};
    float field1_data{42.42};
    std::vector<float> field2_data{42.0, 42.1, 42.2};

    // create the data buffer for the given instance of "my_struct"
    pw::field_buffer_t my_struct_data{field0_data, field1_data, field2_data};

    // call "fill" as usual
    writer.fill("my_struct", my_struct_data);

Since ``field_buffer_t`` is an alias of ``std::vector``, you can also do:

.. code-block:: cpp

    pw::field_buffer_t my_struct_data;
    my_struct_data.push_back(field0_data);
    my_struct_data.push_back(field1_data);
    my_struct_data.push_back(field2_data);

As mentioned above (and as the name implies) the data provided to an instance
of ``field_buffer_t`` must be provided in the order matching that of
the fields in the user-provided layout for the Parquet file.

For example, consider the layout for the following struct-type column:

.. code-block:: json

    {
      "fields": [
        {
          "name": "another_struct", "type": "struct",
          "fields": [
            {"name": "another_field0", "type": "float"},
            {"name": "another_field1", "type": "float"}
          ]
        }
      ]
    }

The above layout specifies a single struct-type column named ``another_struct``,
with two named fields ``another_field0`` and ``another_field1``.
Both of these fields are of type ``float``.
In using the ``field_buffer_t`` approach to writing to the struct,
users must be careful to provide the data in the correct order.
Otherwise inconsistencies in the stored data will emerge.

For example, the below would not be caught as an invalid column write
since the types of the provided data match those specified in the layout
but the intended meaning of the data is lost since the data for
``another_field1`` will be written to the column for ``another_field0``
and vice versa:

.. code-block:: cpp

    float another_field0_data{42.42};
    float another_field1_data{84.84};

    // incorrect order!
    pw::field_buffer_t another_struct_data{another_field1_data, another_field0_data};

Instead of the correct ordering:

.. code-block:: cpp

    // correct order!
    pw::field_buffer_t another_struct_data{another_field0_data, another_field1_data};


