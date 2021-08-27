#pragma once

//parquetwriter
#include "parquet_writer_types.h"

//std/stl
#include <string>
#include <vector>
#include <map>

//json
#include "nlohmann/json.hpp"

//arrow
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/type.h>


namespace parquetwriter {

    enum class Compression {
        UNCOMPRESSED,
        GZIP,
        SNAPPY
    };

    enum class FlushRule {
        N_ROWS,
        BUFFER_SIZE
    };

    class Writer {
        public :
            Writer();
            ~Writer() = default;

            std::string compression2str();
            std::string flushrule2str();

            void initialize_output(const std::string& dataset_name,
                        const std::string& output_directory);

            void load_schema(const std::string& field_layout_str,
                    const std::string& metadata_json_str = "");
            void load_schema(const nlohmann::json& field_layout,
                    const nlohmann::json& metadata = {});

            void new_file();
            void initialize_writer();

            void set_row_group_rule(const FlushRule& rule, const uint32_t& n);
            void set_pagesize(const uint32_t& pagesize) { _data_pagesize = pagesize; }

            void fill(const std::string& field_path,
                    const std::vector<types::buffer_t>& data_buffer);

            void finish();


        private :

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

            // map of each column's builders
            std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> _col_builder_map;

            //
            // methods
            //
            void update_output_stream();

            void flush();

            

    };

}
