# parquet-writer
A C++ library that allows for easily writing Parquet files containing columns of (mostly) whatever type you wish.

## Motivation

The `parquet-writer` library allows you to specify your file layout (i.e. what types
of columns to store) using a simple JSON schema and supports almost all of the numeric
[data types supported by Parquet](https://arrow.apache.org/docs/cpp/api/datatype.html)
as well as `boolean` values.

`parquet-writer` also supports one, two, and three dimensional nesting of the supported types,
as well as supporting storing arbitrary struct-like objects with any
number of fields as long as they are of a supported basic types.

## Basic Usage

The `parquet-writer` library provides users with the `parquetwriter::Writer`
class, which users provide with a JSON object specifying the desired structure
of their output Parquet file and then fill accordingly.
The basic usage is illustrated in the following,

```c++
#include "parquet_writer.h"
#include "nlohmann/json.h"

// JSON from string literal
auto file_layout = R"(
  {
    "fields": [
      {"name": "column0", "type": "float32"},
      {"name": "column1", "type": "int32"}
    ]
  }
)"_json;

// initialize the Parquet writer instance
namespace pw = parquetwriter;
pw::Writer writer;
writer.set_layout(file_layout);
writer.set_dataset_name("my_dataset");
writer.initialize();

// get and fill the data
float column0_value = 42.0;
int32_t column1_value = 42;

// for each row in the output file, fill each of the columns 
// (note the surrounding curly braces)
writer.fill("column0", {column0_value});
writer.fill("column1", {column1_value});

// when finished writing the file, call finish()
writer.finish();
```

## Data Types

### Basic Types

The basic data types that can be written to the output Parquet files
using `parquet-writer` and their correspondence to their C++
types are detailed in the table below,

| C++ type | `parquet-writer` type |
| ---      | ---       |
| `bool`   | `"bool"`  |
| `uint8_t`/`int8_t` | `"uint8"`/`"int8"` |
| `uint16_t`/`int16_t` | `"uint16"`/`"int16"` |
| `uint32_t`/`int32_t` | `"uint32"`/`"int32"` |
| `uint64_t`/`int64_t` | `"uint64"`/`"int64"` |
| `float` | `"float32"`|
| `double` | `"float64"` |

### List Types

Storing one, two, and three dimensional lists of the [basic types](#supported-data-types) is supported
by `parquet-writer`. Specifying lists of these types is done via
the JSON layout provided to a given `parquetwriter::Writer` instance.

For example, the following JSON layout specifies a Parquet file
containing a one-dimensional variable-lengthed list column
named `my_1d_list`, a two-dimensional variable-lengthed list column named `my_2d_list`,
and a three-dimensional list column named `my_3d_list` holding
C++ `float`, `uint32_t`, and `double` types, respectively:
```c++
auto file_layout = R"(
  {
    "fields": [
      {"name": "my_1d_list", "type": "list", "contains": {"type": "float32"}},
      {"name": "my_2d_list",
                "type": "list", "contains":
                        {"type": "list", "contains": {"type": "uint32"}}
      },
      {"name": "my_3d_list",
                "type": "list", "contains":
                       {"type": "list", "contains":
                                {"type": "list", "contains": {"type": "float64"}}
                       }
      }   
    ]
  } 
)"_json;
```
As can be seen in the above, specifying `list` types for output columns requires
an additional `contains` object in the JSON object defining the column. This
`contains` object defines the data type to be stored in the output variable-lengthed
list.

Filling these `list` types with an `parquetwriter::Writer` instance is done
using standard `C++` `std::vector` instances of the associated `C++` type.
For example, taking the example specification above,
```c++
// one-dimensional case
std::vector<float> my_1d_list_data{1.2, 2.3, 3.4};
writer.fill("my_1d_list", {my_1d_list_data});

// two-dimensional case
std::vector<std::vector<uint32_t>> my_2d_list_data{
                                    {42}, {19, 27, 32}, {}, {72, 101}
                                  };
writer.fill("my_2d_list", {my_2d_list_data});

// three-dimensional case
std::vector<std::vector<std::vector<double>>> my_3d_list_data{
                                    { {0.5, 1.2}, {3.0, 4.0, 5.0}, {} },
                                    { {42.0}, {10.23}, {11.34} }
                                  };
writer.fill("my_3d_list", {my_3d_list_data});
```





## Building

Below are the steps to build the `parquet-writer` shared library for your system.

<details>
  <summary>MacOS</summary>
  
  ```
  mkdir build/ && cd build/
  cmake -DARROW_PATH=/usr/local/Cellar/apache-arrow/<version>/ ..
  make
  ```
  Where `<version>` must be replaced by the specific version, e.g. `5.0.0`.
  
</details>
  
<details>
  <summary>Debian/Ubuntu</summary>
  
  ```
  mkdir build/ && cd build/
  cmake -DCMAKE_MODULE_PATH=/usr/lib/<arch>/cmake/arrow/ ..
  make
  ```
  Where `<arch>` may be something like `x86_64-linux-gnu` if present.
  
</details>
  
Upon a successful build, the shared library `parquet-writer` will be located under `build/lib`.
  
It is assumed that you have installed Apache Arrow and Apache Parquet
following the [steps below](#installing-apache-arrow-and-parquet-libraries).


## Installing Apache Arrow and Parquet Libraries

See the [official docs](https://arrow.apache.org/install/) for complete details.
Below are the tested ones:

<details>
  <summary>MacOS</summary>
  
  Via `homebrew`:
  
  ```
  brew install apache-arrow
  ```
  
  Which will install everything under `/usr/local/Cellar/apache-arrow/`.
  
</details>

<details>
  <summary>Debian/Ubuntu</summary>
  
  ```
  sudo apt update
  sudo apt install -y -V ca-certificates lsb-release wget
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  sudo apt update
  sudo apt install -y -V libarrow-dev
  sudo apt install -y -V libparquet-dev
  sudo apt install build-essential
  sudo apt install pkg-config
  ```
</details>
