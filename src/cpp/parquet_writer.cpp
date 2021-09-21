#include "parquet_writer.h"

#include "logging.h"
#include "parquet_helpers.h"

// std/stl
#include <filesystem>
#include <sstream>
#include <algorithm>

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

    // debug print-out
    log->debug("{0} - {1} columns loaded from provided layout:", __PRETTYFUNCTION__, _columns.size());
    auto n_columns = _columns.size();
    for(size_t icolumn = 0; icolumn < n_columns; icolumn++) {
        std::string column_name = _columns.at(icolumn)->name();
        log->devug("{0} -     [{1}/{2}] {3}", __PRETTYFUNCTION__, icolumn+1, n_columns, column_name);
    } // icolumn

    _schema = arrow::schema(_columns);
    _arrays.clear();
    if (!_file_metadata.empty()) {
        this->set_metadata(_file_metadata);
    }
    // create the column -> ArrayBuilder mapping
    _col_builder_map = helpers::col_builder_map_from_fields(_columns);

    auto fill_field_builder_map = helpers::fill_field_builder_map_from_columns(_columns);

    _expected_fields_to_fill.clear();

    log->error("{0} - COL build map size = {1}", __PRETTYFUNCTION__, _col_builder_map.size());
    size_t total_n_to_call_fill_on = 0;
    for(const auto& [col_name, builder_map] : _col_builder_map) {

        log->error("{0} - COLUMN {1} has {2} sub-builders:", __PRETTYFUNCTION__, col_name, builder_map.size());
        for(const auto& [sub_name, sub_builder] : builder_map) {
            log->error("{0} -   --> sub_builder = {1}", __PRETTYFUNCTION__, sub_name);
            if(sub_name.find("/item") != std::string::npos) continue; // don't consider list value_builder
            if(sub_name == col_name) {
                log->error("            ====> SUB_NAME++ = {0}", sub_name);
                total_n_to_call_fill_on++;
                _expected_fields_to_fill.push_back(sub_name);
            } else
            if(sub_builder->type()->id() == arrow::Type::LIST) {
                auto value_builder = dynamic_cast<arrow::ListBuilder*>(sub_builder)->value_builder();

                size_t try_count = 0;
                while(value_builder->type()->id() == arrow::Type::LIST) {
                    if(try_count >= 3) break;
                    auto list_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
                    value_builder = list_builder->value_builder();
                    try_count++;

                }
                if(value_builder->type()->id() == arrow::Type::STRUCT) {
                    total_n_to_call_fill_on++;
                    _expected_fields_to_fill.push_back(sub_name);
                    log->error("            ====> SUB_NAME++ = {0}", sub_name);
                }
            }
            else if(sub_builder->type()->id() == arrow::Type::STRUCT) {
                total_n_to_call_fill_on++;
                _expected_fields_to_fill.push_back(sub_name);
                log->error("            ====> SUB_NAME++ = {0}", sub_name);
            }
            log->error("{0} -        -> {1}", __PRETTYFUNCTION__, sub_name);
        }
    }

    for(const auto& field_to_fill : _expected_fields_to_fill) {
        _expected_field_fill_map[field_to_fill] = 0;
    }
    log->error("{0} - TOTAL REQUIRED CALLS TO FILL PER ROW = {1}", __PRETTYFUNCTION__, total_n_to_call_fill_on);
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

void Writer::fill(const std::string& field_path,
                  const std::vector<types::buffer_t>& data_buffer) {

    log->warn("{0} - FILL CALLED FOR PATH: {1}, data_buffer.size() == {2}", __PRETTYFUNCTION__, field_path, data_buffer.size());
    size_t pos_parent = field_path.find_first_of("/.");
    // size_t pos_slash = field_path.find('/');
    // size_t pos_dot = field_path.find_first_of("./");

    // get the parent column builder for this field
    arrow::ArrayBuilder* builder = nullptr;

    bool is_parent_path = false;
    std::string parent_column_name;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    } else {
        is_parent_path = true;
        parent_column_name = field_path;
    }

    if (_col_builder_map.count(parent_column_name) == 0) {
        std::stringstream err;
        err << "Could not find parent column with name \"" << parent_column_name
            << "\" in loaded builders";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    auto col_map = _col_builder_map.at(parent_column_name);
    if (col_map.count(field_path) == 0) {
        std::stringstream err;
        err << "Cannot fill node with name \"" << field_path
            << "\", it is not in the builder map";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }
    builder = _col_builder_map.at(parent_column_name).at(field_path);

    if(!builder) {
        std::stringstream err;
        err << "Builder for field \"" << field_path << "\" is null!";
        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
        throw std::runtime_error(err.str());
    }

    // auto builder = _col_builder_map.at(field_path);
    auto builder_type = builder->type();
    bool is_struct = builder_type->id() == arrow::Type::STRUCT;
    bool is_list = builder_type->id() == arrow::Type::LIST;

    log->error("{0} - field_path = {1} : is_struct = {2}, is_list = {3}, type = {4}", __PRETTYFUNCTION__, field_path, is_struct, is_list, builder->type()->name());
    for(const auto& [name, builder] : col_map) {
        log->error("{0} - COL_MAP name = {1}", __PRETTYFUNCTION__, name);
    }

    if (data_buffer.size() == 0) {
        // case of filling an empty data buffer
        log->error("{0} - data_buffer.size() == 0");
        this->append_empty_value(field_path);
    }
    //else if (data_buffer.size() > 1) {
    else if (is_list) {
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);

        // filling a list type
        //
        // filling a multivalued column field
        //

            log->error("{0} - data_buffer_size() = {1} and IS LIST LINE = {2}", __PRETTYFUNCTION__, data_buffer.size(), __LINE__);
            // handle the case of a list of structs, which
            // will have a data_buffer looking like:
            // {
            //   {a, b, {c, d}},
            //   {a, b, {c, d}}
            // }
            auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
            PARQUET_THROW_NOT_OK(list_builder->Append());
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
            auto value_builder = list_builder->value_builder();
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
            auto value_type = value_builder->type();
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);

            bool is_list2 = value_type->id() == arrow::Type::LIST;
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
            if (is_list2) {
                log->error("{0} - data_buffer_size() = {1} and IS LIST 2D", __PRETTYFUNCTION__, data_buffer.size());
                // data_buffer looking like:
                // {
                //    {
                //      {a, b, {c, d}},
                //      {a, b, {c, d}},
                //    }
                // }

                auto list2_builder =
                    dynamic_cast<arrow::ListBuilder*>(value_builder);
                if(!list2_builder) {
                    std::stringstream err;
                    err << "Value builder for inner list of 2D list at path \"" << field_path << "\" is null!";
                    log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                    throw std::runtime_error(err.str());
                }
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                PARQUET_THROW_NOT_OK(list2_builder->Append());

                auto value2_builder = list2_builder->value_builder();
                auto value2_type = value2_builder->type();
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);

                bool is_list3 = value2_type->id() == arrow::Type::LIST;
                if (is_list3) {
                    log->error("{0} - data_buffer_size() = {1} and IS LIST 3D", __PRETTYFUNCTION__, data_buffer.size());
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    auto list3_builder =
                        dynamic_cast<arrow::ListBuilder*>(value2_builder);
                    if(!list3_builder) {
                        std::stringstream err;
                        err << "Value builder for inner list of 3D list at path \"" << field_path << "\" is null!";
                        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                        throw std::runtime_error(err.str());
                    }
                    std::cout << "FOO list3_builder = " << list3_builder << std::endl;
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    PARQUET_THROW_NOT_OK(list3_builder->Append());
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    auto value3_builder = list3_builder->value_builder();
                    auto value3_type = value3_builder->type();
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);

                    std::string value_node_name = field_path + "/item/item";
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
        std::cout << "FOO filling inner inner list of 3d list at node " << value_node_name << " (field_path = " << field_path << ")" <<  std::endl;
                    this->fill(value_node_name, {data_buffer.at(0)});
                    //for (size_t i = 0; i < data_buffer.size(); i++) {
        log->error("//{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    //    PARQUET_THROW_NOT_OK(list2_builder->Append());
        log->error("//{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    //    auto ivec = std::get<std::vector<
                    //        std::vector<std::vector<types::buffer_value_t>>>>(
                    //        data_buffer.at(i));
        log->error("//{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    //    for (size_t j = 0; j < ivec.size(); j++) {
                    //        PARQUET_THROW_NOT_OK(list3_builder->Append());
                    //        auto jvec = ivec.at(j);
                    //        for (size_t k = 0; k < jvec.size(); k++) {
                    //            auto element_data = jvec.at(k);
                    //            this->fill(value_node_name, {element_data});
                    //        }

                    //    }  // j
                    //}      // i

                } else {
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    std::string value_node_name = field_path + "/item";
                    log->error("{0} FOO {1} : value_node_name = {2}, data_buffer.size() = {3}", __PRETTYFUNCTION__, __LINE__, value_node_name, data_buffer.size());
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    //PARQUET_THROW_NOT_OK(list2_builder->Append());
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    this->fill(value_node_name, {data_buffer.at(0)});
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                    //for (size_t ielement = 0; ielement < data_buffer.size();
                    //     ielement++) {
                    //    PARQUET_THROW_NOT_OK(list2_builder->Append());
                    //    this->fill(value_node_name, {data_buffer.at(ielement)});


                    //    //auto element_data_vec = std::get<
                    //    //    std::vector<std::vector<types::buffer_value_t>>>(
                    //    //    data_buffer.at(ielement));
                    //    //for (size_t jelement = 0;
                    //    //     jelement < element_data_vec.size(); jelement++) {
                    //    //    auto element_data = element_data_vec.at(jelement);

                    //    //    this->fill(value_node_name, {element_data});
                    //    //}  // jelement
                    //}      // ielement
                }          // 2d list
            } else {
                // if(value_type->id() != arrow::Type::STRUCT) {
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                //    std::stringstream err;
                //    err << "ERROR: Only handle nested list[struct], but you
                //    are trying list[" << value_type->name() << "]"; throw
                //    std::runtime_error(err.str());
                //}

                std::string value_node_name = field_path + "/item";
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
        log->error("{0}      data_buffer.size() = {1}", __PRETTYFUNCTION__, data_buffer.size());
                //auto element_data_vec = std::get<std::vector<types::buffer_value_t>>(data_buffer.at(0));
                //for(size_t ielement = 0; ielement < data_buffer.size(); ielement++) {
                //    auto element_data = std::get<std::vector<types::buffer_value_t>>(data_buffer.at(ielement))
                //}
        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
                this->fill(value_node_name, {data_buffer.at(0)});
//                for (size_t ielement = 0; ielement < data_buffer.size();
//                     ielement++) {
//        log->error("{0}      data_buffer.size() = {1}, ielement = {2}", __PRETTYFUNCTION__, data_buffer.size(), ielement);
//        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
//                    auto element_data_vec =
//                        std::get<std::vector<types::buffer_value_t>>(
//                            data_buffer.at(ielement));
//        log->error("{0} - IS LIST LINE {1}", __PRETTYFUNCTION__, __LINE__);
//                    this->fill(value_node_name, {element_data_vec});
//                }  // ielement
            }      // 1D list
    } else if (is_struct) {
        //} else if (auto val =
        //               std::get_if<std::vector<types::buffer_value_t>>(&data)) {

            log->error("{0} - {1} FILLING STRUCT", __PRETTYFUNCTION__, __LINE__);
            
            if(data_buffer.size() != 1) {
                std::stringstream err;
                err << "Filling struct type field with data buffer size =! 1, but size = " << data_buffer.size() << " (a data buffer of type parquetwriter::struct_t is expected!)";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::runtime_error(err.str());
            }

            struct_t struct_data;
            try {
                struct_data = std::get<struct_t>(data_buffer.at(0));
            } catch(std::exception& e) {
                std::stringstream err;
                err << "Unable to parse struct field data for field \"" << field_path << "\" (expect a data buffer of type parquetwriter::struct_t)";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::runtime_error(e.what());
            }

            //types::buffer_value_vec_t field_data_vec = *val;
            log->error("{0} - data_buffer_size() = {1},  and IS STRUCT, struct_data size = {2}", __PRETTYFUNCTION__, data_buffer.size(), struct_data.size());
            auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);

            auto struct_type = struct_builder->type();
            PARQUET_THROW_NOT_OK(struct_builder->Append());


            auto [num_total_fields, num_fields_nonstruct] =
                helpers::field_nums_from_struct(struct_builder, field_path);
            if(struct_data.size() != num_fields_nonstruct) {
                std::stringstream err;
                err << " [0] Invalid number of data elements provided for "
                       "struct column \""
                    << field_path << "\": expect " << num_fields_nonstruct
                    << ", got " << struct_data.size();
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::runtime_error(err.str());
            }

            this->fill_struct(struct_builder, struct_data, field_path);

            //for (size_t ifield = 0; ifield < num_total_fields; ifield++) {
            //    auto field_builder = struct_builder->child_builder(ifield);
            //    auto field_type = field_builder->type();
            //    auto field_name = struct_type->field(ifield)->name();

            //    if (ifield >= struct_data.size()) break;
            //    if (field_type->id() == arrow::Type::STRUCT) {
            //        ifield--;
            //        continue;
            //    }

            //    types::buffer_t field_data = struct_data.at(ifield);
            //    if (auto val =
            //            std::get_if<types::buffer_value_t>(&field_data)) {
            //        std::stringstream field_node_name;
            //        field_node_name << field_path << "." << field_name;
            //        log->error("{0} - IS_STRUCT calling fill with field_node_name = {1}", __PRETTYFUNCTION__, field_node_name.str());
            //        this->fill(field_node_name.str(), {*val});
            //    }
            //}  // ifield
    //}      // is_struct
    } else if (data_buffer.size() == 1) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
        auto data = data_buffer.at(0);
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
        if (auto val = std::get_if<types::buffer_value_t>(&data)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
            // data is flat (array-like)
            if (auto v = std::get_if<bool>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<bool>(*v, builder);
            } else if (auto v = std::get_if<uint8_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<uint8_t>(*v, builder);
            } else if (auto v = std::get_if<uint16_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<uint16_t>(*v, builder);
            } else if (auto v = std::get_if<uint32_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<uint32_t>(*v, builder);
            } else if (auto v = std::get_if<uint64_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<uint64_t>(*v, builder);
            } else if (auto v = std::get_if<int8_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<int8_t>(*v, builder);
            } else if (auto v = std::get_if<int16_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<int16_t>(*v, builder);
            } else if (auto v = std::get_if<int32_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<int32_t>(*v, builder);
            } else if (auto v = std::get_if<int64_t>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<int64_t>(*v, builder);
            } else if (auto v = std::get_if<float>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<float>(*v, builder);
            } else if (auto v = std::get_if<double>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<double>(*v, builder);
            } else if (auto v = std::get_if<std::vector<bool>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<bool>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<uint8_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<uint8_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<uint16_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<uint16_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<uint32_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<uint32_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<uint64_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<uint64_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<int8_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<int8_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<int16_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
                helpers::fill<std::vector<int16_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<int32_t>>(val)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
        std::cout << "                  FOO builder = " << builder << std::endl;
                helpers::fill<std::vector<int32_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<int64_t>>(val)) {
                helpers::fill<std::vector<int64_t>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<float>>(val)) {
                helpers::fill<std::vector<float>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<double>>(val)) {
                helpers::fill<std::vector<double>>(*v, builder);
            } else if (auto v =
                           std::get_if<std::vector<std::vector<bool>>>(val)) {
                helpers::fill<std::vector<std::vector<bool>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<uint8_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<uint8_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<uint16_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<uint16_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<uint32_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<uint32_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<uint64_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<uint64_t>>>(*v, builder);
            } else if (auto v =
                           std::get_if<std::vector<std::vector<int8_t>>>(val)) {
                helpers::fill<std::vector<std::vector<int8_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<int16_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<int16_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<int32_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<int32_t>>>(*v, builder);
            } else if (auto v = std::get_if<std::vector<std::vector<int64_t>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<int64_t>>>(*v, builder);
            } else if (auto v =
                           std::get_if<std::vector<std::vector<float>>>(val)) {
                helpers::fill<std::vector<std::vector<float>>>(*v, builder);
            } else if (auto v =
                           std::get_if<std::vector<std::vector<double>>>(val)) {
                helpers::fill<std::vector<std::vector<double>>>(*v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<bool>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<bool>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<uint8_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint8_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<uint16_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint16_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<uint32_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint32_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<uint64_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint64_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<int8_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<int8_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<int16_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<int16_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<int32_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<int32_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<int64_t>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<int64_t>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<float>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<float>>>>(
                    *v, builder);
            } else if (auto v = std::get_if<
                           std::vector<std::vector<std::vector<double>>>>(
                           val)) {
                helpers::fill<std::vector<std::vector<std::vector<double>>>>(
                    *v, builder);
            } else {
                std::stringstream err;
                err << "Invalid type, cannot fill";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::logic_error(err.str());
            }
        } else if (auto val =
                       std::get_if<std::vector<types::buffer_value_t>>(&data)) {
        log->error("{0} - data_buffer_size() == 1, LINE == {1}", __PRETTYFUNCTION__, __LINE__);
            // here we handle the case of a data buffer containing a a
            // vector of potentially differently-typed fields (e.g. a struct)

            size_t try_count = 0;
            while (builder_type->id() != arrow::Type::STRUCT) {
                if (try_count > 3) {
                    std::stringstream err;
                    err << "Expected builder type of \"struct\" for field with "
                           "name \""
                        << field_path << "\", got \"" << builder_type->name()
                        << "\"";
                    log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                    throw std::runtime_error(err.str());
                }
                if (builder_type->id() == arrow::Type::LIST) {
                    auto tmp = dynamic_cast<arrow::ListBuilder*>(builder)
                                   ->value_builder();
                    auto tmp_type = tmp->type();
                    // if(tmp_type->id() == arrow::Type::STRUCT) {
                    builder = tmp;
                    builder_type = builder->type();
                    //}
                }
                try_count++;
            }
            log->error("{0} - data_buffer_size() == 1 {1}", __PRETTYFUNCTION__, __LINE__);
            types::buffer_value_vec_t field_data_vec = *val;

            auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
            auto struct_type = struct_builder->type();
            PARQUET_THROW_NOT_OK(struct_builder->Append());

            auto [num_total_fields, num_fields_nonstruct] =
                helpers::field_nums_from_struct(struct_builder, field_path);
            if (field_data_vec.size() != num_fields_nonstruct) {
                std::stringstream err;
                err << " [1] Invalid number of data elements provided for "
                       "struct column \""
                    << field_path << "\": expect " << num_fields_nonstruct
                    << ", got " << field_data_vec.size();
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::runtime_error(err.str());
            }

            for (size_t ifield = 0; ifield < num_total_fields; ifield++) {
                auto field_builder = struct_builder->child_builder(ifield);
                auto field_type = field_builder->type();
                auto field_name = struct_type->field(ifield)->ToString();

                // don't consider inner structs, they must be called
                // manually with a new call to "fill"

                // for structs within structs, you MUST fill from outer to inner
                // e.g.
                //      writer.fill("struct", {struct_data});
                //      writer.fill("struct.inner_struct", {inner_struct_data});
                if (ifield >= field_data_vec.size()) break;
                if (field_type->id() == arrow::Type::STRUCT) {
                    ifield--;
                    continue;
                }

                bool field_ok = false;
                types::buffer_value_t field_data = field_data_vec.at(ifield);
                switch (field_type->id()) {
                    case arrow::Type::BOOL:
                        if (auto v = std::get_if<bool>(&field_data)) {
                            helpers::fill<bool>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT8:
                        if (auto v = std::get_if<uint8_t>(&field_data)) {
                            helpers::fill<uint8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT16:
                        if (auto v = std::get_if<uint16_t>(&field_data)) {
                            helpers::fill<uint16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT32:
                        if (auto v = std::get_if<uint32_t>(&field_data)) {
                            helpers::fill<uint32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT64:
                        if (auto v = std::get_if<uint64_t>(&field_data)) {
                            helpers::fill<uint64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT8:
                        if (auto v = std::get_if<int8_t>(&field_data)) {
                            helpers::fill<int8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT16:
                        if (auto v = std::get_if<int16_t>(&field_data)) {
                            helpers::fill<int16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT32:
                        if (auto v = std::get_if<int32_t>(&field_data)) {
                            helpers::fill<int32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT64:
                        if (auto v = std::get_if<int64_t>(&field_data)) {
                            helpers::fill<int64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::FLOAT:
                        if (auto v = std::get_if<float>(&field_data)) {
                            helpers::fill<float>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::DOUBLE:
                        if (auto v = std::get_if<double>(&field_data)) {
                            helpers::fill<double>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;

                    case arrow::Type::LIST:
                        if (auto v =
                                std::get_if<std::vector<bool>>(&field_data)) {
                            helpers::fill<std::vector<bool>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint8_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint8_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint16_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint16_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint32_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint32_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint64_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint64_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int8_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int8_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int16_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int16_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int32_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int32_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int64_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int64_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<float>>(
                                       &field_data)) {
                            helpers::fill<std::vector<float>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<double>>(
                                       &field_data)) {
                            helpers::fill<std::vector<double>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else
                            // 2D list field
                            if (auto v =
                                    std::get_if<std::vector<std::vector<bool>>>(
                                        &field_data)) {
                            helpers::fill<std::vector<std::vector<bool>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint8_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint8_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint16_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint16_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint32_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint32_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint64_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint64_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int8_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int8_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int16_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int16_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int32_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int32_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int64_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int64_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<float>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<float>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<double>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<double>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<bool>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<bool>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint8_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<uint8_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint16_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint16_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint32_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint32_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint64_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint64_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int8_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int8_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int16_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int16_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int32_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int32_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int64_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int64_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<float>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<float>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<double>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<double>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else {
                            std::stringstream err;
                            err << "Unhandled fill type for struct field";
                            log->error("{0} - {1}", __PRETTYFUNCTION__,
                                       err.str());
                            throw std::runtime_error(err.str());
                        }
                        break;
                    default:
                        std::stringstream err;
                        err << "Could not field field \"" << field_name
                            << "\" for struct \"" << field_path;
                        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                        throw std::runtime_error(err.str());
                        break;
                }  // switch
                if (!field_ok) {
                    std::stringstream err;
                    err << "Failed to fill field \"" << field_name
                        << "\" for struct \"" << field_path;
                    log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                    throw std::runtime_error(err.str());
                }
            }  // ifield
        } else {
            std::stringstream err;
            err << "Invalid data type given to fill method for field at \""
                << field_path << "\"";
            log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
            throw std::runtime_error(err.str());
        }
    }

    if (std::find(_expected_fields_to_fill.begin(), _expected_fields_to_fill.end(), field_path) != _expected_fields_to_fill.end()) {
        _field_fill_count++;
        _expected_field_fill_map.at(field_path)++;
    }

    if(row_complete()) {
        log->error("{0} - ROW COMPLETE!", __PRETTYFUNCTION__);
        _row_length++;
        if(_row_length % _n_rows_in_group == 0) {
            this->flush();
        }
    }

    //if (_field_fill_count % _expected_fields_to_fill.size() == 0) {

    //    log->warn("{0} - number of fields per row filled: _field_fill_count = {1}, _expected_fields_to_fill.size() = {2}", __PRETTYFUNCTION__, _field_fill_count, _expected_fields_to_fill.size());

    //    _row_length++;

    //    if (_fill_count % _n_rows_in_group == 0) {
    //        log->warn("{0} - FLUSHING _fill_count = {1}", _fill_count);
    //        this->flush();
    //    }
    //}
}

void Writer::fill_struct(arrow::StructBuilder* struct_builder, struct_t struct_data, const std::string& field_path) {

    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);
            auto struct_type = struct_builder->type();
    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);
            PARQUET_THROW_NOT_OK(struct_builder->Append());
    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);

            auto [num_total_fields, num_fields_nonstruct] =
                helpers::field_nums_from_struct(struct_builder, field_path);
            if (struct_data.size() != num_fields_nonstruct) {
                std::stringstream err;
                err << " [1] Invalid number of data elements provided for "
                       "struct column \""
                    << field_path << "\": expect " << num_fields_nonstruct
                    << ", got " << struct_data.size();
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::runtime_error(err.str());
            }

    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);
            for (size_t ifield = 0; ifield < num_total_fields; ifield++) {
    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);
                auto field_builder = struct_builder->child_builder(ifield);
                auto field_type = field_builder->type();
                auto field_name = struct_type->field(ifield)->ToString();
    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);

                // don't consider inner structs, they must be called
                // manually with a new call to "fill"

                // for structs within structs, you MUST fill from outer to inner
                // e.g.
                //      writer.fill("struct", {struct_data});
                //      writer.fill("struct.inner_struct", {inner_struct_data});
                if (ifield >= struct_data.size()) break;
                if (field_type->id() == arrow::Type::STRUCT) {
                    ifield--;
                    continue;
                }
    log->error("{0} - FILL_STRUCT LINE = {1}", __PRETTY_FUNCTION__, __LINE__);

                bool field_ok = false;
                types::buffer_value_t field_data = struct_data.at(ifield);
                switch (field_type->id()) {
                    case arrow::Type::BOOL:
                        if (auto v = std::get_if<bool>(&field_data)) {
                            helpers::fill<bool>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT8:
                        if (auto v = std::get_if<uint8_t>(&field_data)) {
                            helpers::fill<uint8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT16:
                        if (auto v = std::get_if<uint16_t>(&field_data)) {
                            helpers::fill<uint16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT32:
                        if (auto v = std::get_if<uint32_t>(&field_data)) {
                            helpers::fill<uint32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT64:
                        if (auto v = std::get_if<uint64_t>(&field_data)) {
                            helpers::fill<uint64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT8:
                        if (auto v = std::get_if<int8_t>(&field_data)) {
                            helpers::fill<int8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT16:
                        if (auto v = std::get_if<int16_t>(&field_data)) {
                            helpers::fill<int16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT32:
                        if (auto v = std::get_if<int32_t>(&field_data)) {
                            helpers::fill<int32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT64:
                        if (auto v = std::get_if<int64_t>(&field_data)) {
                            helpers::fill<int64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::FLOAT:
                        if (auto v = std::get_if<float>(&field_data)) {
                            helpers::fill<float>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::DOUBLE:
                        if (auto v = std::get_if<double>(&field_data)) {
                            helpers::fill<double>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;

                    case arrow::Type::LIST:
                        if (auto v =
                                std::get_if<std::vector<bool>>(&field_data)) {
                            helpers::fill<std::vector<bool>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint8_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint8_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint16_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint16_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint32_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint32_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<uint64_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<uint64_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int8_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int8_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int16_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int16_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int32_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int32_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<int64_t>>(
                                       &field_data)) {
                            helpers::fill<std::vector<int64_t>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<float>>(
                                       &field_data)) {
                            helpers::fill<std::vector<float>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<double>>(
                                       &field_data)) {
                            helpers::fill<std::vector<double>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else
                            // 2D list field
                            if (auto v =
                                    std::get_if<std::vector<std::vector<bool>>>(
                                        &field_data)) {
                            helpers::fill<std::vector<std::vector<bool>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint8_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint8_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint16_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint16_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint32_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint32_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<uint64_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<uint64_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int8_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int8_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int16_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int16_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int32_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int32_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<int64_t>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<int64_t>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<float>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<float>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<
                                       std::vector<std::vector<double>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<std::vector<double>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<bool>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<bool>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint8_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<uint8_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint16_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint16_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint32_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint32_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<uint64_t>>>>(
                                       &field_data)) {
                            helpers::fill<std::vector<
                                std::vector<std::vector<uint64_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int8_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int8_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int16_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int16_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int32_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int32_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<int64_t>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<int64_t>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<float>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<float>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else if (auto v = std::get_if<std::vector<
                                       std::vector<std::vector<double>>>>(
                                       &field_data)) {
                            helpers::fill<
                                std::vector<std::vector<std::vector<double>>>>(
                                *v, field_builder.get());
                            field_ok = true;
                        } else {
                            std::stringstream err;
                            err << "Unhandled fill type for struct field";
                            log->error("{0} - {1}", __PRETTYFUNCTION__,
                                       err.str());
                            throw std::runtime_error(err.str());
                        }
                        break;
                    default:
                        std::stringstream err;
                        err << "Could not field field \"" << field_name
                            << "\" for struct \"" << field_path;
                        log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                        throw std::runtime_error(err.str());
                        break;
                }  // switch
                if (!field_ok) {
                    std::stringstream err;
                    err << "Failed to fill field \"" << field_name
                        << "\" for struct \"" << field_path;
                    log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                    throw std::runtime_error(err.str());
                }
            }  // ifield
}

bool Writer::row_complete() {
    // check that each expected fill field has been filled once
    bool complete = true;
    std::vector<std::string> remainders;
    std::vector<uint64_t> remainders_counts;
    for(const auto& [field_name, field_fill_count] : _expected_field_fill_map) {
        if(field_fill_count != 1) {
            complete = false;
            remainders.push_back(field_name);
            remainders_counts.push_back(field_fill_count);
        }
    }
    return complete;
}

void Writer::append_empty_value(const std::string& field_path) {
    log->error("{0} - APPENDING EMPTY VALUE FOR FIELD {1}", __PRETTYFUNCTION__, field_path);
    arrow::ArrayBuilder* builder = nullptr;
    size_t pos_parent = field_path.find_first_of("/.");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    // auto col_map = _col_builder_map.at(parent_column_name);
    builder = _col_builder_map.at(parent_column_name).at(field_path);
    auto builder_type = builder->type();

    // nested types (list, struct)
    bool is_struct = builder_type->id() == arrow::Type::STRUCT;
    bool is_list = builder_type->id() == arrow::Type::LIST;
    bool is_nested = is_struct || is_list;
    if (!is_nested) {
        PARQUET_THROW_NOT_OK(builder->AppendEmptyValue());
        log->error("{0} -           => !is_nested AppendEmptyValue", __PRETTYFUNCTION__);
    } else if (is_list) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        PARQUET_THROW_NOT_OK(list_builder->AppendEmptyValue());
        log->error("{0} -           => is_list AppendEmptyValue", __PRETTYFUNCTION__);
    } else if (is_struct) {
        auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
        PARQUET_THROW_NOT_OK(struct_builder->AppendEmptyValue());
        log->error("{0} -           => is_struct AppendEmptyValue", __PRETTYFUNCTION__);
    }
}

void Writer::append_null_value(const std::string& field_path) {
    log->error("{0} - APPENDING NULL VALUE FOR FIELD {1}", __PRETTYFUNCTION__, field_path);
    arrow::ArrayBuilder* builder = nullptr;
    size_t pos_parent = field_path.find_first_of("/.");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    // auto col_map = _col_builder_map.at(parent_column_name);
    builder = _col_builder_map.at(parent_column_name).at(field_path);
    auto builder_type = builder->type();

    // nested types (list, struct)
    bool is_struct = builder_type->id() == arrow::Type::STRUCT;
    bool is_list = builder_type->id() == arrow::Type::LIST;
    bool is_nested = is_struct || is_list;
    if (!is_nested) {
        log->error("{0} -           => !is_nested AppendNull", __PRETTYFUNCTION__);
        PARQUET_THROW_NOT_OK(builder->AppendNull());
    } else if (is_list) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        PARQUET_THROW_NOT_OK(list_builder->AppendNull());
        log->error("{0} -           => is_list AppendNull", __PRETTYFUNCTION__);
    } else if (is_struct) {
        auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
        PARQUET_THROW_NOT_OK(struct_builder->AppendNull());
        log->error("{0} -           => is_struct AppendNull", __PRETTYFUNCTION__);
    }
}

void Writer::end_row() {
    log->warn("{0} -------------------------------------", __PRETTYFUNCTION__);
    for (const auto& field : _expected_fields_to_fill) {

        auto field_fill_count = _expected_field_fill_map.at(field);

        log->warn("{0} -   EXPECTED FIELD {1} HAS FILL COUNT = {2}", __PRETTYFUNCTION__, field, field_fill_count);
        if (field_fill_count == 0) {
            this->append_null_value(field);
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
    log->error("{0} - IN FLUSH", __PRETTYFUNCTION__);

    _arrays.clear();
    std::shared_ptr<arrow::Array> array;
    for (auto& column : _columns) {
        PARQUET_THROW_NOT_OK(_col_builder_map.at(column->name())
                                 .at(column->name())
                                 ->Finish(&array));
        _arrays.push_back(array);
    }

    auto table = arrow::Table::Make(_schema, _arrays);
    size_t n_columns = table->num_columns();
    log->error("{0} - LOOPING OVER TABLE COLUMNS", __PRETTYFUNCTION__);
    for(size_t icol = 0; icol <  n_columns; icol++) {
        auto column = table->column(icol);
        log->error("{0} -    COLUMN \"{1}\": LENGTH = {2}", __PRETTYFUNCTION__, table->field(icol)->name(), column->length());

    }
    PARQUET_THROW_NOT_OK(_file_writer->WriteTable(*table, _row_length));
    _row_length = 0;

    // flush to the output file
    PARQUET_THROW_NOT_OK(_output_stream->Flush());
    _arrays.clear();
}

void Writer::finish() {
    log->error("{0} - IN FINISH", __PRETTYFUNCTION__);
    this->flush();
    PARQUET_THROW_NOT_OK(_file_writer->Close());
}

};  // namespace parquetwriter
