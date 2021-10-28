.. _sec:structs_with_structlists:

Storing Structs That Have Struct List Fields
============================================

Storing struct-typed columns that have named fields that are
lists containing elements of type ``struct`` is supported.

Declaring Structs That Have Struct List Fields
----------------------------------------------

Declaring a struct-typed column with a field that is a list of
elements of type ``struct`` is done as follows:

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

The above describes a struct-typed column named ``my_struct``
with two named fields ``field0`` and ``structlist``.

The field ``field0`` holds the basic value type ``float``.
The field ``structlist`` is a one-dimensional list of struct-type
elements each having two named fields ``foo`` and ``bar``.

The above pattern works for two- and three-dimensional lists of struct-typed
elements simply by swapping out the ``list1d`` type for ``list2d`` or
``list3d`` where appropriate.

.. warning::
    The :ref:`struct list constraints<subsec:struct_list_constraints>` still
    hold for the case when the struct list is associated with a
    named field of a ``struct`` typed column.

Writing Structs That Have Struct List Fields
--------------------------------------------

Writing to struct-type columns that contain fields that are lists of
struct-type elements is done similarly to the case
of writing to struct-type columns containing struct-typed fields
by using the dot (``.``) notation for nested struct-types.

For example, assuming the layout declared in the previous section:

.. code-block:: cpp

    namespace pw = parquetwriter;

    // data for the non-struct fields of the struct "my_struct"
    float field0_data{42.0};
    pw::field_map_t my_struct_data{
        {"field0", field0_data}
    };

    // data for the struct-list field named "structlist"
    std::vector<pw::field_map_t> structlist_data;
    for(...) {
        // generate struct field data
        float foo_data{42.42};
        int32_t bar_data{42};

        // create the struct element
        pw::field_map_t struct_data{
            {"foo", foo_data},
            {"bar", bar_data}
        };
        structlist_data.push_back(struct_data);
    }

    // call "fill" using dot notation for nested struct types
    writer.fill("my_struct", my_struct_data);
    writer.fill("my_struct.structlist", structlist_data);

