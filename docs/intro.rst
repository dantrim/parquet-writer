.. _sec:intro:

Introduction
============

The goal of the ``parquet-writer`` library is to make it as easy as possible
to write Parquet files containing data structures that are easily
declared.

Users only need to declare the layout and data types associated
with columns in their Parquet files following a simple JSON schema.
With this information, the
``parquet-writer`` library will know the precise pathways needed
to write the user-provided data to the correct data columns with
the correct structure without the user having to know any of the details
of the C++ API of Apache Arrow or Apache Parquet.
