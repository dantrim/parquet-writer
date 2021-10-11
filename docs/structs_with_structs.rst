.. _sec:structs_with_structs:

Storing Structs That Have Struct Fields
=======================================

``parquet-writer`` also allows for storing 
``struct`` type columns that contain fields that are themselves
of type ``struct``

Declaring Structs with Struct Fields
------------------------------------

Specifying a ``struct`` type column that contains a named field
that is itself of type ``struct``, with it's own additional set of
named fields, is done as follows:

.. code-block:: json

    {
      "fields": [
        {
          "name": "outer_struct", "type": "struct",
          "fields": [
            {"name": "outer_field0", "type": "float"},
            {
              "name": "inner_struct", "type": "struct",
              "fields": [
                {"name": "inner_field0", "type": "float"},
                {"name": "inner_field1", "type": "int32"},
                {"name": "inner_field2", "type": "list1d", "contains": {"type": "float"}}
              ]
            }
          ]
        }
      ]
    }

The above describes a ``struct`` column named ``outer_struct`` which has
two named fields ``outer_field0`` and ``inner_struct``.
The named field ``outer_field0`` is a field having a basic value type ``float``,
whereas the named field ``inner_struct`` is a ``struct`` type field
having three named fields: ``inner_field0``, ``inner_field1``,
and ``inner_field2`` of types ``float``, ``int32``, and ``list1d[float]``,
respectively.

Writing Structs with Struct Fields
----------------------------------

Writing to the column described in the previous section would be done as follows:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // data for the non-struct fields of the struct "outer_struct"
    float outer_field0_data{42.0};
    pw::field_map_t outer_struct_data{
        {"outer_field0", outer_field0_data}
    };

    // data for the non-struct fields of the struct "inner_struct"
    float inner_field0_data{42.0};
    int32_t inner_field1_data{42};
    std::vector<float> inner_field2_data{42.0, 42.1, 42.2};
    pw::field_map_t inner_struct{
        {"inner_field0", inner_field0_data},
        {"inner_field1", inner_field1_data},
        {"inner_field2", inner_field2_data}
    };

    // call "fill" for each struct
    writer.fill("outer_struct", outer_struct_data);
    writer.fill("outer_struct.inner_struct", inner_struct_data);

As can be seen, for each level of nesing of ``struct`` typed columns/fields,
one provides a ``field_map_t`` (or ``field_buffer_t``) instance containing
the data for all fields that are not of ``struct`` type.

Internal named fields that are of type ``struct`` are written to using the dot (``.``)
notation in the call to ``fill`` with the
convention ``<outer_struct_name>.<inner_struct_name>``, as seen
in the above: ``writer.fill("outer_struct.inner_struct", ...)``.

.. _subsec:struct_struct_constraints:

Constraints
-----------

For simplicity, any named field of type ``struct`` of a ``struct`` type column
is not itself allowed to have a field of type ``struct``:

.. note::
    A column of type ``struct`` cannot itself contain named fields of
    type ``struct`` that have fields of type ``struct``.

So, for example, the following Parquet file layout declaration is not allowed:

.. code-block:: json

    {
      "fields": [
        {
          "name": "struct0", "type": "struct",
          "fields": [
            {"name": "field0", "type": "float"},
            {"name": "struct1", "type": "struct",
             "fields": [
                {"name": "inner_field0", "type": "float"},
                {"name": "struct2", "type": "struct",
                 "fields": [
                    {"name": "inner_inner_field0", "type": "float"}
                  ]
                }
              ]
            }
          ]
      ]
    }

The above is disallowed since the inner struct ``struct1`` contains
a ``struct`` typed field ``struct2``.
