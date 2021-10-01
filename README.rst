
|GitHub Actions Status: CI| |GitHub Actions Status: check-format| |ReadTheDocs|

Easily declare and write Parquet files
======================================

The idea is for ``parquet-writer`` to make it simple to both
specify the desired layout of a Parquet file (i.e. the
number and structure of data columns) and to subsequently
write your data to that file.

In summary, ``parquet-writer`` provides support for:

  * Specifying the layout of Parquet files using JSON
  * Storing numeric and boolean data types to output Parquet files
  * Storing struct objects (think: ``C/C++`` structs) having any number of arbitrarily-typed fields
  * Storing 1, 2, and 3 dimensional lists of the supported data types
  * A simple interface for writing the supported data types to Parquet files

The Basics
----------

``parquet-writer`` provides users with the ``parquetwriter::Writer``
class, which they provide with a JSON object specifying the desired
"layout" of their Parquet files and then fill accordingly.

An example JSON layout, stored in the file ``layout.json``, could be:

.. code:: json

    {
        "fields": [
            {"name": "foo", "type": "float"},
            {"name": "bar", "type": "uint32"},
            {"name": "baz", "type": "list1d", "contains": {"type": "float"}}
        ]
    }

The above describes an output Parquet file containing three data columns
named ``foo``, ``bar``, and ``baz`` which contain data of types ``float``
(32-bit precision float), ``uint32`` (32-bit unsigned integer), and
``list[float]`` (variable-lengthed 1-dimensional list of elements of type ``float``), respectively.

The basics of initializing a ``parquetwriter::Writer`` instance with the above layout,
writing some values to a single row, and storing the output is below:

.. code:: cpp

    #include "parquet_writer.h"

    namespace pw = parquetwriter;
    pw::Writer writer;
    std::ifstream layout_file("layout.json"); // file containing JSON layout spec
    writer.set_layout(layout_file);
    writer.set_dataset("my_dataset"); // must give a name to the output
    writer.initialize();

    // generate some data for each of the columns
    float foo_data{42.0};
    uint32_t bar_data{42};
    std::vector<float> baz_data{42.0, 42.1, 42.2, 42.3};

    // call "fill" for each of the columns, giving the associated data
    writer.fill("foo", foo_data);
    writer.fill("bar", bar_data);
    writer.fill("baz", baz_data);

    // signal the end of a row
    writer.end_row();

    // call "finish" when done writing to the file
    writer.finish();

The above would generate an output file called ``my_dataset.parquet``.
We can use `parquet-tools <https://pypi.org/project/parquet-tools/>`_ 
to quickly dump the contents of the Parquet file:

.. code:: shell

    $ parquet-tools show my_dataset.parquet
    +------+------+--------------------------+
    | foo  | bar  | baz                      |
    |------+------+--------------------------|
    | 42.0 | 42   | [42.0, 42.1, 42.2, 42.3] |
    +------+------+--------------------------+


..
.. LINKS

.. |GitHub Actions Status: CI| image:: https://github.com/dantrim/parquet-writer/workflows/CI/badge.svg?branch=main
   :target: https://github.com/dantrim/parquet-writer/actions?query=workflow%3ACI+branch%3Amain

.. |GitHub Actions Status: check-format| image:: https://github.com/dantrim/parquet-writer/workflows/check-format/badge.svg?branch=main
   :target: https://github.com/dantrim/parquet-writer/actions?query=workflow%3Acheck-format+branch%3Amain

.. |ReadTheDocs| image:: https://readthedocs.org/projects/parquet-writer/badge/?version=latest
   :target: https://parquet-writer.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

