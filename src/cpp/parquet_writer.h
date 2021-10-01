#pragma once

// parquetwriter
#include "logging.h"
#include "parquet_writer_fill_types.h"
#include "parquet_writer_types.h"
namespace spdlog {
class logger;
}

// std/stl
#include <fstream>
#include <map>
#include <string>
#include <vector>

// json
#include "nlohmann/json.hpp"

// arrow
#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/type.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

/**
 A test struct named `foo`
 */
struct foo {
    /**
     A test function

     \param val An input value
     \return Whether or not the value is valid
     */
    bool bar(const std::string& val);
};

namespace parquetwriter {

enum class Compression { UNCOMPRESSED, GZIP, SNAPPY };

enum class FlushRule { NROWS, BUFFERSIZE };

class Writer {
 public:
    Writer();
    ~Writer() = default;

    // set the name of the output dataset
    void set_dataset_name(const std::string& dataset_name);

    // set the path to the output directory in which to store the output Parquet
    // file(s)
    void set_output_directory(const std::string& output_directory);

    //
    // methods to provide the JSON layout for the output Parquet file(s)
    //

    // layout provided as an input file
    void set_layout(std::ifstream& infile);

    // layout provided as a serialized JSON string
    void set_layout(const std::string& field_layout_str);

    // layout provided directly as an instance of nlohmann::json
    void set_layout(const nlohmann::json& field_layout);

    //
    // methods to provide the JSON containing the file metadata
    //

    // metadata provided as an input file
    void set_metadata(std::ifstream& infile);

    // metadata provided as a serialized JSON string
    void set_metadata(const std::string& metdata_str);

    // metadata provided directly as an instance of nlohmann::json
    void set_metadata(const nlohmann::json& metadata);

    // set the output Parquet file compression algorithm
    void set_compression(const Compression& compression) {
        _compression = compression;
    }

    // set the rule governing how the data is flushed to the output file
    void set_flush_rule(const FlushRule& rule, const uint32_t& n);

    // set the Parquet file page size
    void set_pagesize(const uint32_t& pagesize) { _data_pagesize = pagesize; }

    // get the set compression algorithm
    const Compression& compression() { return _compression; }

    // get the set flush rule
    const FlushRule& flushrule() { return _flush_rule; }

    // get the provided compression algorithm as std::string instance
    static const std::string compression2str(const Compression& compression);

    // get the provided flush rule as a std::string instance
    static const std::string flushrule2str(const FlushRule& flush_rule);

    // instantiate the Parquet file writer with the loaded layout, metadata, and
    // other specific configuration
    void initialize();

    //
    // methods for writing to output columns
    //

    // write to value_type and list[value_type] columns
    void fill(const std::string& field_path, const value_t& data_value);

    // write to a struct column using field_buffer_t input
    void fill(const std::string& field_path, const field_buffer_t& struct_data);

    // write to a list1d[struct] column using field_buffer_t input
    void fill(const std::string& field_path,
              const std::vector<field_buffer_t>& struct_list_data);

    // write to a list2d[struct] column using field_buffer_t input
    void fill(const std::string& field_path,
              const std::vector<std::vector<field_buffer_t>>& struct_list_data);

    // write to a list3d[struct] column using field_buffer_t input
    void fill(const std::string& field_path,
              const std::vector<std::vector<std::vector<field_buffer_t>>>&
                  struct_list_data);

    // write to a struct column using field_map_t input
    void fill(const std::string& field_path, const field_map_t& struct_data);

    // write to a list1d[struct] column using field_map_t input
    void fill(const std::string& field_path,
              const std::vector<field_map_t>& struct_list_data);

    // write to a list2d[struct] column using field_map_t input
    void fill(const std::string& field_path,
              const std::vector<std::vector<field_map_t>>& struct_list_data);

    // write to a list3d[struct] column using field_map_t input
    void fill(const std::string& field_path,
              const std::vector<std::vector<std::vector<field_map_t>>>&
                  struct_list_data);

    // call AppendEmptyValue on a column \"field_path\"
    void append_empty_value(const std::string& field_path);

    // call AppendNullValue on a column \"field_path\"
    void append_null_value(const std::string& field_path);

    // signal that writing to a given row has finished
    void end_row();

    // writing to the output file has finished
    void finish();

 private:
    // Parquet output wrtier
    std::unique_ptr<parquet::arrow::FileWriter> _file_writer;
    std::shared_ptr<arrow::fs::FileSystem> _fs;
    std::shared_ptr<arrow::fs::SubTreeFileSystem> _internal_fs;
    std::shared_ptr<arrow::io::OutputStream> _output_stream;

    // output location and name
    std::string _output_directory;
    std::string _dataset_name;

    // the index of the current file being written to (useful for
    // cases where the output dataset is partitioned into multiple files)
    uint32_t _file_count;

    // the number of rows in a given RowGroup to write in the output
    // Parquet file
    int64_t _n_rows_in_group;

    // this map holds the number of times any of the columns/fields expected to
    // be passed to Writer::fill(...) have been filled for a given output row
    // before writing to the output Parquet file (i.e. before Writer::end_row()
    // has been called)
    std::map<std::string, uint64_t> _expected_field_fill_map;

    // the names of the columns/fields that should have Writer::fill(...) called
    // on them for a given output row
    std::vector<std::string> _expected_fields_to_fill;

    // the mapping between the columns/fields expected to be passed to
    // Writer::fill(...) and the type of column (or layout) it is
    std::map<std::string, FillType> _expected_fields_filltype_map;

    // the number of rows that have been filled and that are waiting to be
    // written to the output Parquet file
    uint32_t _n_current_rows_filled;

    // the set compression algorithm for the output Parquet file
    Compression _compression;

    // the set flush rule algorithm
    FlushRule _flush_rule;

    // the set data pagesize for the output Parquet file
    uint32_t _data_pagesize;

    // layout of the output Parquet File
    std::shared_ptr<arrow::Schema> _schema;
    std::vector<std::shared_ptr<arrow::Field>> _columns;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    nlohmann::json _file_metadata;

    // mapping between each of the columns/fields expected to be passed to
    // Writer::fill(...) and its associated top-level ArrayBuilder
    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
        _column_builder_map;

    //
    // methods
    //
    void update_output_stream();
    void new_file();

    // signals that filling of a given column/field has been completed
    void end_fill(const std::string& field_path);

    // returns the fill order of fields (by name) associated with a given
    // struct-type column
    std::vector<std::string> struct_fill_order(const std::string& field_path);

    // fill an instance of a value_type and list[value_type] data element
    void fill_value(const std::string& field_name, arrow::ArrayBuilder* builder,
                    const value_t& input_data);

    // fill an instance of a struct-type data element
    void fill_struct(const std::string& field_path,
                     arrow::ArrayBuilder* builder,
                     const std::vector<value_t>& struct_field_data);

    // construct a properly-ordered field_buffer_t from an instance of
    // field_map_t
    field_buffer_t field_map_to_field_buffer(
        const std::string& field_path, const field_map_t& struct_field_map);

    // increment the field fill counter for the given column/field
    void increment_field_fill_count(const std::string& field_path);

    // check if the currently active row has had all of its columns/fields
    // written too
    void check_row_complete();
    bool row_is_complete();
    void flush_if_ready();

    // flush the current in-memory data (rows) to the output Parquet file
    void flush();

    //
    // logging
    //
    std::shared_ptr<spdlog::logger> log;
};

}  // namespace parquetwriter
