.. _sec:struct_list_types:

Storing Lists of Structs
========================

Storing lists containing elements that are of type ``struct`` is supported.

.. Storing lists of struct-type columns and fields are supported,
.. and can be constructed by building up instances of
.. ``std::vector`` containing elements of either
.. ``parquetwriter::field_map_t`` or ``parquetwriter::field_buffer_t``.


Declaring Lists of Structs
--------------------------

Declaring columns that contain lists whose elements are of type ``struct``
is done by composing the :ref:`list type<sec:list_types>`
and :ref:`struct type<sec:struct_types>` declarations.

For example, the following declares a one-dimensional list containing
struct-type elements that have three named fields:

.. code-block:: json

    {
      "fields": [
        {
          "name": "structlist", "type": "list1d",
          "contains": { "type": "struct",
                        "fields": [
                          {"name": "field0", "type": "float"},
                          {"name": "field1", "type": "int32"},
                          {"name": "field2", "type": "list1d", "contains": {"type": "float"}}
                        ]
                      }
        }
      ]
    }

To declare two- or three-dimensional lists,
one would simply swap the ``type`` field for the ``structlist`` column
from ``list1d`` to either ``list2d`` or ``list3d``.


Writing Lists of Structs
------------------------

Writing to columns that contain lists of struct-type elements is done by
building up instances of ``std::vector`` containing elements of either
:ref:`field_map_t<sec:struct_field_map>` or :ref:`field_buffer_t<sec:struct_field_buffer>`.

For example, writing a one-dimensional list containing the three-field struct elements
described above would be done as follows:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // 1D vector of struct elements
    std::vector<pw::field_map_t> structlist_data;

    // fill the 1D vector with struct data elements
    for(...) {
        // generate struct field data
        float field0_data{42.42};
        int32_t field1_data{42};
        std::vector<float> field2_data{42.0, 42.1, 42.2};

        // create the struct element
        pw::field_map_t struct_data{
            {"field0", field0_data},
            {"field1", field1_data},
            {"field2", field2_data}
        };

        // append to the struct list
        structlist_data.push_back(struct_data);
    }

    // call "fill" as usual
    writer.fill("structlist", structlist_data);

The two-dimensional case:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // 2D vector of struct elements
    std::vector<std::vector<pw::field_map_t>> structlist_data;

    // fill the 2D vector with struct data elements
    for(...) {
        std::vector<pw::field_map_t> inner_structlist_data;
        for(...) {
            pw::field_map_t struct_data{
                {"field0", field0_data},
                {"field1", field1_data},
                {"field2", field2_data}
            };
            inner_structlist_data.push_back(struct_data);
        }
        structlist_data.push_back(inner_structlist_data);
    }

    // call "fill" as usual
    writer.fill("structlist", structlist_data);

And the three-dimensional case:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // 3D vector of struct elements
    std::vector<std::vector<std::vector<pw::field_map_t>>> structlist_data;

    // fill the 3D vector with struct data elements
    for(...) {
        std::vector<std::vector<pw::field_map_t>> inner_structlist_data;
        for(...) {
            std::vector<pw::field_map_t> inner_inner_structlist_data;
            for(...) {
                pw::field_map_t struct_data{
                    {"field0", field0_data},
                    {"field1", field1_data},
                    {"field2", field2_data}
                };
                inner_inner_structlist_data.push_back(struct_data);
            }
            inner_structlist_data.push_back(inner_inner_structlist_data);
        }
        structlist_data.push_back(inner_structlist_data);
    }

    // call "fill" as usual
    writer.fill("structlist", structlist_data);


.. _subsec:struct_list_constraints:

Constraints
-----------

.. warning::
    The ``struct`` type elements contained in lists of ``struct`` cannot
    themselves contain fields that are of type ``struct``.

For simplicity, any list type data column whose elements are of type ``struct``,
cannot contain ``struct`` type elements that have
fields that are themselves of type ``struct``.

For example, the following Parquet file layout declaration is not allowed:

.. code-block:: json

    {
      "fields": [
        {
          "name": "structlist",
          "type": "list1d",
          "contains": {
            "type": "struct",
            "fields": [
              {"name": "field0", "type": "float"},
              {
               "name": "inner_struct", "type": "struct",
               "fields": [{"name": "inner_field0", "type": "float"}]
              }
            ]
          }
      ]         
    }

.. note::
    The above ``list1d`` type column is not allowd since its ``struct`` typed
    elements are declared as having an internal ``struct`` typed column (the field named
    ``inner_struct``).

