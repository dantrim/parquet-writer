#pragma once

// parquetwriter
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

    void fill(const std::string& field_path,
              const std::vector<types::buffer_t>& data_buffer);

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

    uint32_t _fill_count;
    uint32_t _field_fill_count;
    uint32_t _row_length;

    Compression _compression;
    FlushRule _flush_rule;

    uint32_t _data_pagesize;

    // layout of the output Parquet File
    std::shared_ptr<arrow::Schema> _schema;
    std::vector<std::shared_ptr<arrow::Field>> _fields;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    nlohmann::json _file_metadata;

    // map of each column's builders
    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
        _col_builder_map;

    //
    // methods
    //
    void update_output_stream();
    void new_file();

    void flush();

    //
    // logging
    //
    std::shared_ptr<spdlog::logger> log;
};

}  // namespace parquetwriter
