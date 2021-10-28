.. _sec:building:

Building ``parquet-writer``
===========================


.. _sec:installation:

Installation
------------

Below are steps to build the ``parquet-writer`` library using CMake
on various architectures.

It is assumed that you have :ref:`installed Apache Arrow and Parquet<sec:install_deps>`
before following the procedures below.

Upon a successful build, the shared library ``parquet-writer`` will
be located under ``build/lib``.


macOS
^^^^^

.. code-block::

    mkdir build && cd build
    cmake -DARROW_PATH=$(brew --prefix apache-arrow) ..
    make


Debian/Ubuntu
^^^^^^^^^^^^^

.. code-block::

    mkdir build && cd build
    cmake -DCMAKE_MODULE_PATH=$(find /usr/lib -type d -name arrow) ..
    make

.. _sec:install_deps:

Installing Apache Arrow and Parquet
-----------------------------------

The ``parquet-writer`` library naturally depends on the Apache Arrow
and Apache Parquet libraries.
Below are reproduced the necessary steps to install the dependencies
on various architectures.
See the `official documentation <https://arrow.apache.org/install/>`_ for further
details.

macOS
^^^^^

.. code-block::

    brew install apache-arrow

Debian/Ubuntu
^^^^^^^^^^^^^

.. code-block::

    apt install -y -V lsb-release wget pkg-config
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt-get update -y
    apt-get install -y libarrow-dev=5.0.0-1 libparquet-dev=5.0.0-1
