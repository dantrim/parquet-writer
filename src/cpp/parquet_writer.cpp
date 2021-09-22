#include "parquet_writer.h"

#include "parquet_helpers.h"

// std/stl
#include <algorithm>
#include <filesystem>
#include <sstream>

// arrow/parquet

namespace parquetwriter {

Writer::Writer()
    : _output_directory("./"),
      _dataset_name(""),
      _file_count(0),
      _row_length(0),
      _field_fill_count(0),
      _total_fills_required_per_row(0),
      _n_rows_in_group(-1),
      _compression(Compression::UNCOMPRESSED),
      _flush_rule(FlushRule::NROWS),
      _data_pagesize(1024 * 1024 * 512) {
    log = logging::get_logger();
}

const std::string Writer::compression2str(const Compression& compression) {
    std::string out = "";
    switch (compression) {
        case Compression::UNCOMPRESSED: {
            out = "UNCOMPRESSED";
            break;
        }
        case Compression::GZIP: {
            out = "GZIP";
            break;
        }
        case Compression::SNAPPY: {
            out = "SNAPPY";
            break;
        }
    }
    return out;
}

const std::string Writer::flushrule2str(const FlushRule& flush_rule) {
    std::string out = "";
    switch (flush_rule) {
        case FlushRule::NROWS: {
            out = "N_ROWS";
            break;
        }
        case FlushRule::BUFFERSIZE: {
            out = "BUFFER_SIZE";
            break;
        }
    }
    return out;
}

void Writer::set_layout(std::ifstream& infile) {
    nlohmann::json jlayout;
    infile.seekg(0);
    try {
        jlayout = nlohmann::json::parse(infile);
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Failed to parse JSON from provided input filestream";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(e.what());
    }
    this->set_layout(jlayout);
}

void Writer::set_layout(const std::string& field_layout_json_str) {
    nlohmann::json jlayout;
    try {
        jlayout = nlohmann::json::parse(field_layout_json_str);
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Failed to parse JSON from provided std::string";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(e.what());
    }
    this->set_layout(jlayout);
}

void Writer::set_layout(const nlohmann::json& field_layout) {
    // there must be a top-level "fields" node

    _columns = helpers::columns_from_json(field_layout);
    if (_columns.size() == 0) {
        std::stringstream err;
        err << "No fields constructed from provided file layout";
        log->error("{0} - {1}", __PRETTYFUNCTION__,
                   "No fields constructed from provided file layout");
        throw std::runtime_error(err.str());
    }

    _schema = arrow::schema(_columns);
    _arrays.clear();
    if (!_file_metadata.empty()) {
        this->set_metadata(_file_metadata);
    }

    // create the column -> ArrayBuilder mapping
    std::tie(_expected_fields_to_fill, _column_builder_map) =
        helpers::fill_field_builder_map_from_columns(_columns);

    _expected_fields_filltype_map.clear();

    log->debug("{0} - ============================================",
               __PRETTYFUNCTION__);
    log->debug("{0} - Loaded fill_field_builder_map (size = {1}):",
               __PRETTYFUNCTION__, _column_builder_map.size());
    size_t total_fills_per_row = 0;
    size_t icolumn = 0;
    for (const auto& column : _columns) {
        std::string col_name = column->name();
        auto col_builder_map = _column_builder_map.at(col_name);
        log->debug("{0} - Column #{1}: {2}", __PRETTYFUNCTION__, icolumn++,
                   col_name);
        for (const auto& [sub_name, sub_builder] : col_builder_map) {
            log->debug("{0} -      {1}: type = {2}, fill_type = {3}",
                       __PRETTYFUNCTION__, sub_name,
                       sub_builder->type()->name(),
                       filltype_to_string(helpers::column_filltype_from_builder(
                           sub_builder, sub_name)));
            total_fills_per_row++;

            _expected_fields_filltype_map[sub_name] =
                helpers::column_filltype_from_builder(sub_builder, sub_name);
        }
    }

    for (const auto& field_to_fill : _expected_fields_to_fill) {
        _expected_field_fill_map[field_to_fill] = 0;
    }
}

void Writer::set_metadata(std::ifstream& infile) {
    infile.seekg(0);
    try {
        _file_metadata = nlohmann::json::parse(infile);
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Failed to parse JSON from provided filestream";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(e.what());
    }

    if (_schema) {
        this->set_metadata(_file_metadata);
    }
}

void Writer::set_metadata(const std::string& metadata_str) {
    if (metadata_str.empty()) return;
    try {
        _file_metadata = nlohmann::json::parse(metadata_str);
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Failed to parse JSON from provided std::string";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(e.what());
    }

    if (_schema) {
        this->set_metadata(_file_metadata);
    }
}

void Writer::set_metadata(const nlohmann::json& metadata) {
    _file_metadata = metadata;
    if (_file_metadata.count("metadata") == 0) {
        std::stringstream err;
        err << "Metadata JSON top-level \"metdata\" node not found";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    if (_schema) {
        std::unordered_map<std::string, std::string> metadata_map;
        metadata_map["metadata"] = metadata["metadata"].dump();
        arrow::KeyValueMetadata keyval_metadata(metadata_map);
        _schema = _schema->WithMetadata(keyval_metadata.Copy());
    }
}

void Writer::set_dataset_name(const std::string& dataset_name) {
    if (dataset_name.empty()) {
        std::stringstream err;
        err << "ERROR: Attempting to give output dataset an invalid name: \"\"";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }
    _dataset_name = dataset_name;
}

void Writer::set_output_directory(const std::string& output_directory) {
    _output_directory = output_directory;
}

void Writer::new_file() {
    this->update_output_stream();
    this->initialize();
}

void Writer::update_output_stream() {
    std::stringstream output_filename;
    output_filename << _dataset_name << "_" << std::setfill('0') << std::setw(4)
                    << _file_count << ".parquet";
    PARQUET_ASSIGN_OR_THROW(
        _output_stream, _internal_fs->OpenOutputStream(output_filename.str()));
    _file_count++;
}

void Writer::initialize() {
    if (_dataset_name.empty()) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty dataset name";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::logic_error(err.str());
    }

    if (!_schema) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty Parquet schema";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::logic_error(err.str());
    }

    if (_columns.size() == 0) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty layout (no columns "
               "specified)";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::logic_error(err.str());
    }

    //
    // create the output path and filesystem handler
    //
    std::string internal_path;
    _fs = arrow::fs::FileSystemFromUriOrPath(
              std::filesystem::absolute(_output_directory), &internal_path)
              .ValueOrDie();
    PARQUET_THROW_NOT_OK(_fs->CreateDir(internal_path));
    _internal_fs =
        std::make_shared<arrow::fs::SubTreeFileSystem>(internal_path, _fs);

    // create the output stream at the new location
    update_output_stream();

    //
    // default RowGroup specification for now (need to make configurable)
    //
    if (_n_rows_in_group < 0) {
        _n_rows_in_group = 250000 / _expected_fields_to_fill.size();
    }

    //
    // create the Parquet writer instance
    //

    auto compression = arrow::Compression::UNCOMPRESSED;
    switch (_compression) {
        case Compression::UNCOMPRESSED: {
            compression = arrow::Compression::UNCOMPRESSED;
            break;
        }
        case Compression::GZIP: {
            compression = arrow::Compression::GZIP;
            break;
        }
        case Compression::SNAPPY: {
            compression = arrow::Compression::SNAPPY;
            break;
        }
    };

    auto writer_properties = parquet::WriterProperties::Builder()
                                 .compression(compression)
                                 ->data_pagesize(_data_pagesize)
                                 ->build();

    auto arrow_writer_properties =
        parquet::ArrowWriterProperties::Builder().store_schema()->build();
    PARQUET_THROW_NOT_OK(parquet::arrow::FileWriter::Open(
        *_schema, arrow::default_memory_pool(), _output_stream,
        writer_properties, arrow_writer_properties, &_file_writer));
}

void Writer::set_flush_rule(const FlushRule& rule, const uint32_t& n) {
    if (rule == FlushRule::BUFFERSIZE) {
        std::stringstream err;
        err << "FlushRule::BUFFERSIZE is not supported";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }
    _flush_rule = rule;
    _n_rows_in_group = n;
}

void Writer::fill_value(const std::string& field_name,
                        arrow::ArrayBuilder* builder,
                        const std::vector<types::buffer_t>& data_buffer) {
    if (data_buffer.size() != 1) {
        std::stringstream err;
        err << "Filling field \"" << field_name
            << "\" with expected fill type FillType::VALUE with data buffer "
               "size != 1 (size = "
            << data_buffer.size() << ")";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error("Invalid data buffer shape");
    }

    auto data = data_buffer.at(0);
    if (auto val = std::get_if<types::buffer_value_t>(&data)) {
        if (auto v = std::get_if<bool>(val)) {
            helpers::fill<bool>(*v, builder);
        } else if (auto v = std::get_if<uint8_t>(val)) {
            helpers::fill<uint8_t>(*v, builder);
        } else if (auto v = std::get_if<uint16_t>(val)) {
            helpers::fill<uint16_t>(*v, builder);
        } else if (auto v = std::get_if<uint32_t>(val)) {
            helpers::fill<uint32_t>(*v, builder);
        } else if (auto v = std::get_if<uint64_t>(val)) {
            helpers::fill<uint64_t>(*v, builder);
        } else if (auto v = std::get_if<int8_t>(val)) {
            helpers::fill<int8_t>(*v, builder);
        } else if (auto v = std::get_if<int16_t>(val)) {
            helpers::fill<int16_t>(*v, builder);
        } else if (auto v = std::get_if<int32_t>(val)) {
            helpers::fill<int32_t>(*v, builder);
        } else if (auto v = std::get_if<int64_t>(val)) {
            helpers::fill<int64_t>(*v, builder);
        } else if (auto v = std::get_if<float>(val)) {
            helpers::fill<float>(*v, builder);
        } else if (auto v = std::get_if<double>(val)) {
            helpers::fill<double>(*v, builder);
        } else if (auto v = std::get_if<std::vector<bool>>(val)) {
            helpers::fill<std::vector<bool>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<uint8_t>>(val)) {
            helpers::fill<std::vector<uint8_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<uint16_t>>(val)) {
            helpers::fill<std::vector<uint16_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<uint32_t>>(val)) {
            helpers::fill<std::vector<uint32_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<uint64_t>>(val)) {
            helpers::fill<std::vector<uint64_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<int8_t>>(val)) {
            helpers::fill<std::vector<int8_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<int16_t>>(val)) {
            helpers::fill<std::vector<int16_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<int32_t>>(val)) {
            helpers::fill<std::vector<int32_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<int64_t>>(val)) {
            helpers::fill<std::vector<int64_t>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<float>>(val)) {
            helpers::fill<std::vector<float>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<double>>(val)) {
            helpers::fill<std::vector<double>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<std::vector<bool>>>(val)) {
            helpers::fill<std::vector<std::vector<bool>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<uint8_t>>>(val)) {
            helpers::fill<std::vector<std::vector<uint8_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<uint16_t>>>(val)) {
            helpers::fill<std::vector<std::vector<uint16_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<uint32_t>>>(val)) {
            helpers::fill<std::vector<std::vector<uint32_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<uint64_t>>>(val)) {
            helpers::fill<std::vector<std::vector<uint64_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<int8_t>>>(val)) {
            helpers::fill<std::vector<std::vector<int8_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<int16_t>>>(val)) {
            helpers::fill<std::vector<std::vector<int16_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<int32_t>>>(val)) {
            helpers::fill<std::vector<std::vector<int32_t>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<int64_t>>>(val)) {
            helpers::fill<std::vector<std::vector<int64_t>>>(*v, builder);
        } else if (auto v = std::get_if<std::vector<std::vector<float>>>(val)) {
            helpers::fill<std::vector<std::vector<float>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<double>>>(val)) {
            helpers::fill<std::vector<std::vector<double>>>(*v, builder);
        } else if (auto v =
                       std::get_if<std::vector<std::vector<std::vector<bool>>>>(
                           val)) {
            helpers::fill<std::vector<std::vector<std::vector<bool>>>>(*v,
                                                                       builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<uint8_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<uint8_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<uint16_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<uint16_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<uint32_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<uint32_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<uint64_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<uint64_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<int8_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<int8_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<int16_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<int16_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<int32_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<int32_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<int64_t>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<int64_t>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<float>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<float>>>>(
                *v, builder);
        } else if (auto v = std::get_if<
                       std::vector<std::vector<std::vector<double>>>>(val)) {
            helpers::fill<std::vector<std::vector<std::vector<double>>>>(
                *v, builder);
        } else {
            std::stringstream err;
            err << "Invalid data type in data buffer provided for field \""
                << field_name << "\"";
            log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
            throw std::runtime_error(err.str());
        }
    } else {
        std::stringstream err;
        err << "Could not get data buffer value for filling field \""
            << field_name << "\"";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }
}

void Writer::fill_value_list(const std::string& field_name,
                             arrow::ArrayBuilder* builder,
                             const std::vector<types::buffer_t>& data_buffer) {
    if (data_buffer.size() != 1) {
        std::stringstream err;
        err << "Filling field \"" << field_name
            << "\" with expected fill type FillType::VALUE_LIST_1D with data "
               "buffer  size != 1 (size = "
            << data_buffer.size() << ")";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error("Invalid data buffer shape");
    }

    auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
    this->fill_value(field_name, list_builder, data_buffer);
}

void Writer::fill_struct_list(const std::string& field_name,
                              arrow::ArrayBuilder* builder,
                              const std::vector<types::buffer_t>& data_buffer) {
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
    auto [depth, terminal_builder] =
        helpers::list_builder_description(list_builder);

    if (depth == 1) {
        // initiate a new list element
        PARQUET_THROW_NOT_OK(list_builder->Append());

        // initiate a new struct element
        auto value_builder = list_builder->value_builder();
        for (size_t i = 0; i < data_buffer.size(); i++) {
            struct_t struct_data = helpers::struct_from_data_buffer_element(
                data_buffer.at(i), field_name);
            this->fill_struct(field_name, value_builder, {struct_data});
        }
    } else if (depth == 2) {
        PARQUET_THROW_NOT_OK(list_builder->Append());
        list_builder =
            dynamic_cast<arrow::ListBuilder*>(list_builder->value_builder());
        auto value_builder = list_builder->value_builder();
        for (size_t i = 0; i < data_buffer.size(); i++) {
            PARQUET_THROW_NOT_OK(list_builder->Append());
            auto inner_data =
                std::get<std::vector<struct_t>>(data_buffer.at(i));
            for (size_t j = 0; j < inner_data.size(); j++) {
                struct_t struct_data = helpers::struct_from_data_buffer_element(
                    inner_data.at(j), field_name);
                this->fill_struct(field_name, value_builder, {struct_data});
            }
        }
    } else if (depth == 3) {
        PARQUET_THROW_NOT_OK(list_builder->Append());
        auto inner_list_builder =
            dynamic_cast<arrow::ListBuilder*>(list_builder->value_builder());
        auto inner_inner_list_builder = dynamic_cast<arrow::ListBuilder*>(
            inner_list_builder->value_builder());
        auto value_builder = inner_inner_list_builder->value_builder();
        for (size_t i = 0; i < data_buffer.size(); i++) {
            PARQUET_THROW_NOT_OK(inner_list_builder->Append());
            auto inner_data =
                std::get<std::vector<std::vector<struct_t>>>(data_buffer.at(i));
            for (size_t j = 0; j < inner_data.size(); j++) {
                PARQUET_THROW_NOT_OK(inner_inner_list_builder->Append());
                auto inner_inner_data = inner_data.at(
                    j);  // std::get<std::vector<struct_t>>(inner_data.at(j));
                for (size_t k = 0; k < inner_inner_data.size(); k++) {
                    struct_t struct_data =
                        helpers::struct_from_data_buffer_element(
                            inner_inner_data.at(k), field_name);
                    this->fill_struct(field_name, value_builder, {struct_data});
                }  // k
            }      // j
        }          // i
    }
}

void Writer::fill_struct(const std::string& field_name,
                         arrow::ArrayBuilder* builder,
                         const std::vector<types::buffer_t>& data_buffer) {
    //
    // get the struct data
    //
    struct_t struct_data;
    try {
        struct_data = std::get<struct_t>(data_buffer.at(0));
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Unable to parse struct field data for field \"" << field_name
            << "\" (data buffer does not contain type "
               "parquetwriter::struct_t!)";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error("Invalid data buffer shape");
    }

    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
    auto [num_total_fields, num_fields_nonstruct] =
        helpers::field_nums_from_struct(struct_builder, field_name);

    if (struct_data.size() != num_fields_nonstruct) {
        std::stringstream err;
        err << "Invalid number of data elements provided for struct at \""
            << field_name << "\" (got: " << struct_data.size()
            << ", expect: " << num_fields_nonstruct << ")";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error("Invalid data buffer shape");
    }

    // initiate a new struct element
    PARQUET_THROW_NOT_OK(struct_builder->Append());

    //
    // here will fill the fields of the struct, assuming that the order and type
    // of the data in the provided data_buffer/struct_t matches that of the
    // actual column type
    //
    auto struct_type = struct_builder->type();
    for (size_t ifield = 0; ifield < num_total_fields; ifield++) {
        auto field_builder = struct_builder->child_builder(ifield).get();
        auto field_type = field_builder->type();
        auto field_name = struct_type->field(ifield)->name();

        if (ifield >= struct_data.size()) break;
        if (field_type->id() == arrow::Type::STRUCT) {
            ifield--;
            continue;
        }

        std::stringstream path_name;
        path_name << field_name << "/" << field_name;
        this->fill_value(path_name.str(), field_builder,
                         {struct_data.at(ifield)});
    }  // ifield
}

void Writer::fill(const std::string& field_path,
                  const std::vector<types::buffer_t>& data_buffer) {
    if (_expected_fields_filltype_map.count(field_path) == 0) {
        std::stringstream err;
        err << "Attempting to fill unknown field \"" << field_path << "\"";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    //
    // get the parent column name (needed for cases in which this is a
    // sub-field)
    //
    size_t pos_parent = field_path.find_first_of("/.");
    bool is_parent_path = false;
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }
    if (_column_builder_map.count(parent_column_name) == 0) {
        std::stringstream err;
        err << "Could not find parent column associated with field \""
            << field_path << "\"";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    auto builder = _column_builder_map.at(parent_column_name).at(field_path);
    if (!builder) {
        std::stringstream err;
        err << "Builder for field \"" << field_path << "\" is null!";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    //
    // perform the fill for the corresponding supported type
    //
    auto field_fill_type = _expected_fields_filltype_map.at(field_path);

    if (data_buffer.size() == 0) {
        // the only time that an empty data buffer is valid is if this is a list
        // type field
        if (field_fill_type == FillType::VALUE ||
            field_fill_type == FillType::STRUCT) {
            std::stringstream err;
            err << "Empty data buffer provided for non-list field \""
                << field_path << "\"";
            log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
            throw std::runtime_error("Invalid data buffer shape");
        }
        this->append_empty_value(field_path);
    } else {
        switch (field_fill_type) {
            case FillType::VALUE:
                this->fill_value(field_path, builder, data_buffer);
                break;
            case FillType::VALUE_LIST_1D:
            case FillType::VALUE_LIST_2D:
            case FillType::VALUE_LIST_3D:
                this->fill_value_list(field_path, builder, data_buffer);
                break;
            case FillType::STRUCT:
                this->fill_struct(field_path, builder, data_buffer);
                break;
            case FillType::STRUCT_LIST_1D:
            case FillType::STRUCT_LIST_2D:
            case FillType::STRUCT_LIST_3D:
                this->fill_struct_list(field_path, builder, data_buffer);
                break;
            default:
                throw std::runtime_error("Invalid FillType \"" +
                                         filltype_to_string(field_fill_type) +
                                         "\"");
                break;
        }  // switch
    }

    if (std::find(_expected_fields_to_fill.begin(),
                  _expected_fields_to_fill.end(),
                  field_path) != _expected_fields_to_fill.end()) {
        _field_fill_count++;
        _expected_field_fill_map.at(field_path)++;
    }

    // signal that the row is complete by incremented the current row length,
    // flush if necessary
    if (row_complete()) {
        _row_length++;
        if (_row_length % _n_rows_in_group == 0) {
            this->flush();
        }
    }
}

bool Writer::row_complete() {
    // check that each expected fill field has been filled once
    bool complete = true;
    std::vector<std::string> remainders;
    std::vector<uint64_t> remainders_counts;
    for (const auto& [field_name, field_fill_count] :
         _expected_field_fill_map) {
        if (field_fill_count != 1) {
            complete = false;
            remainders.push_back(field_name);
            remainders_counts.push_back(field_fill_count);
        }
    }
    return complete;
}

void Writer::append_empty_value(const std::string& field_path) {
    arrow::ArrayBuilder* builder = nullptr;
    size_t pos_parent = field_path.find_first_of("/.");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    auto parent_count = _expected_field_fill_map.at(parent_column_name);
    for (const auto [sub_field_name, sub_field_count] :
         _expected_field_fill_map) {
        if (sub_field_name == parent_column_name) continue;
        std::stringstream sub_find;
        sub_find << parent_column_name << ".";
        bool is_sub_field_of_parent =
            sub_field_name.find(sub_find.str()) != std::string::npos;
        if (is_sub_field_of_parent) {
            // this is a sub-field of the current parent field
            // if(sub_field_count >= parent_count) {
            if (sub_field_count != parent_count) {
                std::stringstream err;
                err << "Cannot append EMPTY VALUE to field \"" << field_path
                    << "\", there are unequal fill counts between parent field "
                       "\""
                    << parent_column_name << "\" and the child field \""
                    << field_path
                    << "\" (parent and child fields must have \"fill\" called "
                       "on them an equal number of times!)";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::logic_error("Bad column filling");
            }
        }
    }

    builder = _column_builder_map.at(parent_column_name).at(field_path);
    auto builder_type = builder->type();

    // in principle we could go through the sub-fields of the StructBuilder
    // and ListBuilder to append null/empty to them -- however, here we just
    // append null/empty to the parent builder which appends null/empty
    // to all child builders. this may make the most sense so that we don't
    // get into situations where we have to deal with the logic of
    // offsets between parent and child fields

    // nested types (list, struct)
    bool is_struct = builder_type->id() == arrow::Type::STRUCT;
    bool is_list = builder_type->id() == arrow::Type::LIST;
    bool is_nested = is_struct || is_list;
    if (!is_nested) {
        PARQUET_THROW_NOT_OK(builder->AppendEmptyValue());
    } else if (is_list) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        PARQUET_THROW_NOT_OK(list_builder->AppendEmptyValue());
    } else if (is_struct) {
        auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
        PARQUET_THROW_NOT_OK(struct_builder->AppendEmptyValue());
    }
}

void Writer::append_null_value(const std::string& field_path) {
    arrow::ArrayBuilder* builder = nullptr;
    size_t pos_parent = field_path.find_first_of("/.");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    // if this is a sub-struct field, then assume that the parent
    // column builder is already appending a NULL value --
    // we should not append a null value to a sub-field since then
    // the offsets will be incorrect (the NULL value added to the parent
    // already adds a null to the sub-field)

    auto parent_count = _expected_field_fill_map.at(parent_column_name);
    for (const auto [sub_field_name, sub_field_count] :
         _expected_field_fill_map) {
        if (sub_field_name == parent_column_name) continue;
        std::stringstream sub_find;
        sub_find << parent_column_name << ".";
        bool is_sub_field_of_parent =
            sub_field_name.find(sub_find.str()) != std::string::npos;
        if (is_sub_field_of_parent) {
            // this is a sub-field of the current parent field
            // if(sub_field_count >= parent_count) {
            if (sub_field_count != parent_count) {
                std::stringstream err;
                err << "Cannot append NULL to field \"" << field_path
                    << "\", there are unequal fill counts between parent field "
                       "\""
                    << parent_column_name << "\" and the child field \""
                    << field_path
                    << "\" (parent and child fields must have \"fill\" called "
                       "on them an equal number of times!)";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::logic_error("Bad column filling");
            }
        }
    }

    // auto col_map = _column_builder_map.at(parent_column_name);
    builder = _column_builder_map.at(parent_column_name).at(field_path);
    auto builder_type = builder->type();

    // nested types (list, struct)
    bool is_struct = builder_type->id() == arrow::Type::STRUCT;
    bool is_list = builder_type->id() == arrow::Type::LIST;
    bool is_nested = is_struct || is_list;
    if (!is_nested) {
        PARQUET_THROW_NOT_OK(builder->AppendNull());
    } else if (is_list) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        PARQUET_THROW_NOT_OK(list_builder->AppendNull());
    } else if (is_struct) {
        auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
        PARQUET_THROW_NOT_OK(struct_builder->AppendNull());
    }
}

void Writer::end_row() {
    for (const auto& field : _expected_fields_to_fill) {
        auto field_fill_count = _expected_field_fill_map.at(field);

        if (field_fill_count == 0) {
            _row_length++;
            this->append_null_value(field);
            if (_row_length % _n_rows_in_group == 0) {
                this->flush();
            }
        } else if (field_fill_count > 1) {
            std::stringstream err;
            err << "Column field \"" << field << "\" has been filled "
                << field_fill_count
                << " times for a single row (should be 1), did you forget to "
                   "call Writer::end_row() at some point?";
            throw std::runtime_error(err.str());
        }
    }

    // reset column field fill counts
    for (const auto& field_name : _expected_fields_to_fill) {
        _expected_field_fill_map.at(field_name) = 0;
    }
}

void Writer::flush() {
    _arrays.clear();
    std::shared_ptr<arrow::Array> array;
    for (auto& column : _columns) {
        PARQUET_THROW_NOT_OK(_column_builder_map.at(column->name())
                                 .at(column->name())
                                 ->Finish(&array));
        _arrays.push_back(array);
    }

    auto table = arrow::Table::Make(_schema, _arrays);
    PARQUET_THROW_NOT_OK(_file_writer->WriteTable(*table, _row_length));
    _row_length = 0;

    // flush to the output file
    PARQUET_THROW_NOT_OK(_output_stream->Flush());
    _arrays.clear();
}

void Writer::finish() {
    this->flush();
    PARQUET_THROW_NOT_OK(_file_writer->Close());
}

};  // namespace parquetwriter
