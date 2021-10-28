.. _sec:misc:

Miscellaneous
====================


Adding File Metadata
--------------------

Arbitrary metadata can be stored in JSON format and added to the
output Parquet file using the ``parquet-writer`` library.

This is done by first creating a JSON object containing whatever
arbitrary information you wish and providing it to your
``parquetwriter::Writer`` instance.
Suppose we have the file ``metadata.json`` containing the following JSON:

.. code-block:: json

    {
        "dataset_name": "example_dataset",
        "foo": "bar",
        "creation_date": "2021/10/11",
        "bar": {
            "faz": "baz"
        }
    }

We would pass this to our writer instance as follows:

.. code-block:: cpp

    std::ifstream metadata_file("metadata.json");
    writer.set_metadata(metadta_file);

The above stores the contained JSON to the Parquet file as an instance of
``key:value`` pairs.

The example Python script `dump-metadata.py <https://github.com/dantrim/parquet-writer/blob/main/examples/python/dump-metadata.py>`_
(requires `pyarrow <https://pypi.org/project/pyarrow/>`_) that extracts the metadata stored
by ``parquet-writer`` shows how to extract the metadata and can be run as follows:

.. code-block:: 

    $ python examples/python/dump-metadta.py <file>

where ``<file>`` is a Parquet file written by ``parquet-writer``.

Running `dump-metadata.py <https://github.com/dantrim/parquet-writer/blob/main/examples/python/dump-metadata.py>`_
on a file with the metadata from above woudl look like:

.. code-block:: shell

    $ python examples/python/dump-metadata.py example_file.parquet
    {
        "dataset_name": "example_dataset",
        "foo": "bar",
        "creation_date": "2021/10/11",
        "bar": {
            "faz": "baz"
        }
    }
