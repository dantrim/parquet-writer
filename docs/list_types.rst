.. _sec:list_types:

Storing Lists of Basic Value Types
==================================

Storing one, two, and three dimensional lists whose elements
are any of the :ref:`basic value types<sec:value_types>` is supported
by ``parquet-writer``.
The table below describes the naming convention for the list type columns:

.. _tab:list_types:

+--------------------------+-----------------------------+
| List Type                | ``parquet-writer`` name     |
+==========================+=============================+
| One dimensional list     | ``list1d``                  |
+--------------------------+-----------------------------+
| Two dimensional list     | ``list2d``                  |
+--------------------------+-----------------------------+
| Three dimensional list   | ``list3d``                  |
+--------------------------+-----------------------------+

Declaring List Type Columns
---------------------------

Declaring columns whose data type is a list of the basic value types
in JSON is done as follows,

.. code-block:: json

    {
      "fields": [
        {"name": "column0", "type": "list1d", "contains": {"type": "float"}},
        {"name": "column1", "type": "list2d", "contains": {"type": "uint32"}},
        {"name": "column2", "type": "list3d", "contains": {"type": "double"}}
      ]
    }

As can be seen in the above, declaring list types for output data columns
requires an additional ``contains`` object in the JSON declaration of the column.
The ``contains`` object defines the data type to be stored in the output,
variable-lengthed list.

Writing List Type Columns
-------------------------

Writing to list type columns is done by using instances of ``std::vector``
for the C++ type associated with the ``type`` field in the ``contains``
object of the list type column's declaration.

For example, taking the layout declaration from the previous section:

.. code-block:: cpp

    // data for the 1D list column named "column0"
    std::vector<float> column0_data{1.2, 2.3, 3.4, 4.3};

    // data for the 2D list column named "column1"
    std::vector<std::vector<uint32_t>> column1_data{
                                      {1}, {2, 2}, {3, 3, 3}
                                };

    // data for the 3D list column named "column2"
    std::vector<std::vector<std::vector<double>>> column2_data{
                                    { {1.1}, {2.2, 2.2}, {3.3, 3.3, 3.3} },
                                    { {3.1, 3.1, 3.1}, {2.2, 2.2}, {1.1} }
                                };

    // at some point call "fill"
    writer.fill("column0", column0_data);
    writer.fill("column1", column1_data);
    writer.fill("column2", column2_data);


Columns of list type can be variably lengthed. 
That is, rows of list type columns do not all have to have the same
number of contained elements (no padding is necessary).
Indeed, one row of a given list column
can have many elements while the next row is empty, for example:

.. code-block:: cpp

    // fill a row's "column0" field with length 3 1D list
    std::vector<float> column0_data{1.2, 2.3, 3.4};
    writer.fill("column0", column0_data);
    writer.end_row();

    // fill a row's "column0" field with a length 0 1D list
    column0_data.clear();
    writer.fill("column0", column_data);
    writer.end_row();




