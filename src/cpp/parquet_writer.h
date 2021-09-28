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

namespace parquetwriter {

enum class Compression { UNCOMPRESSED, GZIP, SNAPPY };

enum class FlushRule { NROWS, BUFFERSIZE };

class Writer {
 public:
    Writer();
    ~Writer() = default;

    void set_dataset_name(const std::string& dataset_name);
    void set_output_directory(const std::string& output_directory);

    void set_layout(std::ifstream& infile);
    void set_layout(const std::string& field_layout_str);
    void set_layout(const nlohmann::json& field_layout);

    void set_metadata(std::ifstream& infile);
    void set_metadata(const std::string& metdata_str);
    void set_metadata(const nlohmann::json& metadata);

    void set_compression(const Compression& compression) {
        _compression = compression;
    }

    void initialize();

    void set_flush_rule(const FlushRule& rule, const uint32_t& n);
    void set_pagesize(const uint32_t& pagesize) { _data_pagesize = pagesize; }

    // handles the filling of all value_types and list[<value_type>] filling
    void fill(const std::string& field_path, const value_t& data_value);

    // handles the filling of a struct and struct list objects
    void fill(const std::string& field_path, const field_buffer_t& struct_data);
    void fill(const std::string& field_path,
              const std::vector<field_buffer_t>& struct_list_data);
    void fill(const std::string& field_path,
              const std::vector<std::vector<field_buffer_t>>& struct_list_data);
    void fill(const std::string& field_path,
              const std::vector<std::vector<std::vector<field_buffer_t>>>&
                  struct_list_data);

    void fill(const std::string& field_path, const field_map_t& struct_data);
    void fill(const std::string& field_path,
              const std::vector<field_map_t>& struct_list_data);
    void fill(const std::string& field_path,
              const std::vector<std::vector<field_map_t>>& struct_list_data);
    void fill(const std::string& field_path,
              const std::vector<std::vector<std::vector<field_map_t>>>&
                  struct_list_data);

    void append_empty_value(const std::string& field_path);
    void append_null_value(const std::string& field_path);
    void end_row();
    void finish();

    const Compression& compression() { return _compression; }
    const FlushRule& flushrule() { return _flush_rule; }

    static const std::string compression2str(const Compression& compression);
    static const std::string flushrule2str(const FlushRule& flush_rule);

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

    std::map<std::string, uint64_t> _column_fill_map;
    std::map<std::string, uint64_t> _expected_field_fill_map;
    std::vector<std::string> _expected_fields_to_fill;
    std::map<std::string, FillType> _expected_fields_filltype_map;

    uint32_t _n_current_rows_filled;

    Compression _compression;
    FlushRule _flush_rule;

    uint32_t _data_pagesize;

    // layout of the output Parquet File
    std::shared_ptr<arrow::Schema> _schema;
    std::vector<std::shared_ptr<arrow::Field>> _columns;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    nlohmann::json _file_metadata;

    // map of each column's builders
    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
        _column_builder_map;

    //
    // methods
    //
    void update_output_stream();
    void new_file();

    void end_fill(const std::string& field_path);
    std::vector<std::string> struct_fill_order(const std::string& field_path);
    void fill_value(const std::string& field_name, arrow::ArrayBuilder* builder,
                    const value_t& data_buffer);
    void fill_value_list(const std::string& field_name,
                         arrow::ArrayBuilder* builder,
                         const value_t& data_buffer);
    void fill_struct(const std::string& field_path,
                     arrow::ArrayBuilder* builder,
                     const std::vector<value_t>& struct_field_data);
    field_buffer_t field_map_to_field_buffer(
        const std::string& field_path, const field_map_t& struct_field_map);

    std::pair<FillType, arrow::ArrayBuilder*> initialize_fill(
        const std::string& field_path, const FillType& expected_filltype);

    void increment_field_fill_count(const std::string& field_path);
    void check_row_complete();
    bool row_is_complete();
    void flush_if_ready();
    void flush();

    //
    // logging
    //
    std::shared_ptr<spdlog::logger> log;
};

}  // namespace parquetwriter
