#include "parquet_writer.h"
#include "parquet_helpers.h"

#include <iostream>
//std/stl
#include <sstream>
#include <filesystem>

//arrow/parquet


namespace parquetwriter {

Writer::Writer() :
    _output_directory("./"),
    _dataset_name(""),
    _file_count(0),
    _fill_count(0),
    _row_length(0),
    _field_fill_count(0),
    _n_rows_in_group(-1),
    _compression(Compression::UNCOMPRESSED),
    _flush_rule(FlushRule::NROWS),
    _data_pagesize(1024*1024*512)
{}

const std::string Writer::compression2str(const Compression& compression) {
    std::string out = "";
    switch(compression) {
        case Compression::UNCOMPRESSED: { out = "UNCOMPRESSED"; break; }
        case Compression::GZIP: { out = "GZIP"; break; }
        case Compression::SNAPPY: { out = "SNAPPY"; break; }
    }
    return out;
}

const std::string Writer::flushrule2str(const FlushRule& flush_rule) {
    std::string out = "";
    switch(flush_rule) {
        case FlushRule::NROWS: { out = "N_ROWS"; break; }
        case FlushRule::BUFFERSIZE: { out = "BUFFER_SIZE"; break; }
    }
    return out;
}

void Writer::set_layout(const std::string& field_layout_json_str) {

    try {
        auto jlayout = nlohmann::json::parse(field_layout_json_str);
        this->set_layout(jlayout);
    } catch(std::exception& e) {
        std::stringstream err;
        err << "ERROR: Failed to parse provided JSON string specifying the field layout, JSON exception caught: " << e.what();
        throw std::runtime_error(err.str());
    }
}

void Writer::set_layout(const nlohmann::json& field_layout) {

    _fields = helpers::fields_from_json(field_layout);
    for(auto& f : _fields) {
    }
    _schema = arrow::schema(_fields);
    _arrays.clear();
    if(!_file_metadata.empty()) {
        this->set_metadata(_file_metadata);
    }
    // create the column -> ArrayBuilder mapping
    _col_builder_map = helpers::col_builder_map_from_fields(_fields);

}

void Writer::set_metadata(const std::string& metadata_str) {

    if(metadata_str.empty()) return;
    try {
        _file_metadata = nlohmann::json::parse(metadata_str);
        if(_schema) {
            this->set_metadata(_file_metadata);
        }
    } catch(std::exception& e) {
        std::stringstream err;
        err << "ERROR: Failed to parse provided JSON string specifying the file metadata, JSON exception caught: " << e.what();
        throw std::runtime_error(err.str());
    }
}

void Writer::set_metadata(const nlohmann::json& metadata) {

    _file_metadata = metadata;
    if (_file_metadata.count("metadata") == 0) {
        std::stringstream err;
        err << "ERROR: Metadata JSON top-level \"metdata\" node not found";
        throw std::runtime_error(err.str());
    }

    if(_schema) {
        std::unordered_map<std::string, std::string> metadata_map;
        metadata_map["metadata"] = metadata["metadata"].dump();
        arrow::KeyValueMetadata keyval_metadata(metadata_map);
        _schema = _schema->WithMetadata(keyval_metadata.Copy());
    }

}

void Writer::set_dataset_name(const std::string& dataset_name) {

    if(dataset_name.empty()) {
        std::stringstream err;
        err << "ERROR: Attempting to give output dataset an invalid name: \"\"";
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
    output_filename << _dataset_name << "_" << std::setfill('0') << std::setw(4) << _file_count << ".parquet";
    PARQUET_ASSIGN_OR_THROW(
            _output_stream,
            _internal_fs->OpenOutputStream(output_filename.str())
    );
    _file_count++;
}

void Writer::initialize() {

    if(_dataset_name.empty()) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty dataset name";
        throw std::logic_error(err.str());
    }

    if(!_schema) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty Parquet schema";
        throw std::logic_error(err.str());
    }

    if(_fields.size() == 0) {
        std::stringstream err;
        err << "ERROR: Cannot initialize writer with empty layout (no columns specified)";
        throw std::logic_error(err.str());
    }

    //
    // create the output path and filesystem handler
    //
    std::string internal_path;
    _fs = arrow::fs::FileSystemFromUriOrPath(std::filesystem::absolute(_output_directory), &internal_path).ValueOrDie();
    PARQUET_THROW_NOT_OK(_fs->CreateDir(internal_path));
    _internal_fs = std::make_shared<arrow::fs::SubTreeFileSystem>(internal_path, _fs);

    // create the output stream at the new location
    update_output_stream();

    //
    // default RowGroup specification for now (need to make configurable) 
    //
    if(_n_rows_in_group < 0) {
        _n_rows_in_group = 250000 / _fields.size();
    }

    //
    // create the Parquet writer instance
    //

    auto compression = arrow::Compression::UNCOMPRESSED;
    switch(_compression) {
        case Compression::UNCOMPRESSED: { compression = arrow::Compression::UNCOMPRESSED; break; }
        case Compression::GZIP: { compression = arrow::Compression::GZIP; break; }
        case Compression::SNAPPY: { compression = arrow::Compression::SNAPPY; break; }
    };

    auto writer_properties = parquet::WriterProperties::Builder()
        .compression(compression)
        ->data_pagesize(_data_pagesize)
        ->build();

    auto arrow_writer_properties = parquet::ArrowWriterProperties::Builder()
        .store_schema()->build();
    PARQUET_THROW_NOT_OK(parquet::arrow::FileWriter::Open(*_schema,
                arrow::default_memory_pool(),
                _output_stream,
                writer_properties,
                arrow_writer_properties,
                &_file_writer));
}

void Writer::set_flush_rule(const FlushRule& rule, const uint32_t& n) {

    if(rule == FlushRule::BUFFERSIZE) {
        throw std::runtime_error("ERROR: FlushRule::BUFFERSIZE is not supported");
    }
    _flush_rule = rule;
    _n_rows_in_group = n;

}

void Writer::fill(const std::string& field_path,
        const std::vector<types::buffer_t>& data_buffer) {


    if(data_buffer.size() == 0) {
        std::stringstream err;
        err << "ERROR: Cannot fill node \"" << field_path << "\" with empty data buffer";
        throw std::runtime_error(err.str());
    }

    size_t pos_parent = field_path.find_first_of("/.");
    //size_t pos_slash = field_path.find('/');
    //size_t pos_dot = field_path.find_first_of("./");

    // get the parent column builder for this field
    arrow::ArrayBuilder* builder = nullptr;

    std::string parent_column_name;
    if(pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    } else {
        parent_column_name = field_path;
    }

    if(_col_builder_map.count(parent_column_name) == 0) {
        std::stringstream err;
        err << "ERROR: Could not find parent column with name \"" << parent_column_name << "\" in loaded builders";
        throw std::runtime_error(err.str());
    }

    auto col_map = _col_builder_map.at(parent_column_name);
    if(col_map.count(field_path) == 0) {
        std::stringstream err;
        err << "ERROR: Cannot fill node with name \"" << field_path << "\", it is not in the builder map";
        throw std::runtime_error(err.str());
    }
    builder = _col_builder_map.at(parent_column_name).at(field_path);

    //auto builder = _col_builder_map.at(field_path);
    auto builder_type = builder->type();

    //
    // filling a multivalued column field
    //
    if(data_buffer.size() > 1) {

        bool is_struct = builder_type->id() == arrow::Type::STRUCT;
        bool is_list = builder_type->id() == arrow::Type::LIST;

        if(is_list) {
            // handle the case of a list of structs, which
            // will have a data_buffer looking like:
            // { 
            //   {a, b, {c, d}},
            //   {a, b, {c, d}} 
            // }
            auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
            PARQUET_THROW_NOT_OK(list_builder->Append());
            auto value_builder = list_builder->value_builder();
            auto value_type = value_builder->type();

            bool is_list2 = value_type->id() == arrow::Type::LIST;
            if(is_list2) {
                // data_buffer looking like:
                // {
                //    { 
                //      {a, b, {c, d}},
                //      {a, b, {c, d}},
                //    }
                // }

                auto list2_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
                auto value2_builder = list2_builder->value_builder();
                auto value2_type = value2_builder->type();

                bool is_list3 = value2_type->id() == arrow::Type::LIST;
                if(is_list3) {

                    auto list3_builder = dynamic_cast<arrow::ListBuilder*>(value2_builder);
                    auto value3_builder = list3_builder->value_builder();
                    auto value3_type = value3_builder->type();

                    std::string value_node_name = field_path + "/item";
                    for(size_t i = 0; i < data_buffer.size(); i++) {
                        PARQUET_THROW_NOT_OK(list2_builder->Append());
                        auto ivec = std::get<std::vector<std::vector<std::vector<types::buffer_value_t>>>>(data_buffer.at(i));
                        for(size_t j = 0; j < ivec.size(); j++) {
                            PARQUET_THROW_NOT_OK(list3_builder->Append());
                            auto jvec = ivec.at(j);
                            for(size_t k = 0; k < jvec.size(); k++) {
                                auto element_data = jvec.at(k);
                                this->fill(value_node_name, {element_data});
                            }

                        } // j
                    } // i

                } else {
                    std::string value_node_name = field_path + "/item";
                    for(size_t ielement = 0; ielement < data_buffer.size(); ielement++) {
                        PARQUET_THROW_NOT_OK(list2_builder->Append());
                        auto element_data_vec = std::get<std::vector<std::vector<types::buffer_value_t>>>(data_buffer.at(ielement));
                        for(size_t jelement = 0; jelement < element_data_vec.size(); jelement++) {
                            auto element_data = element_data_vec.at(jelement);


                            this->fill(value_node_name, {element_data});
                        } // jelement
                    } // ielement
                } // 2d list
            } else {

            //if(value_type->id() != arrow::Type::STRUCT) {
            //    std::stringstream err;
            //    err << "ERROR: Only handle nested list[struct], but you are trying list[" << value_type->name() << "]";
            //    throw std::runtime_error(err.str());
            //}

                std::string value_node_name = field_path + "/item";
                for(size_t ielement = 0; ielement < data_buffer.size(); ielement++) {
                    auto element_data_vec = std::get<std::vector<types::buffer_value_t>>(data_buffer.at(ielement));
                    this->fill(value_node_name, {element_data_vec});
                } // ielement
            } // 1D list
        } else
        if(is_struct) {
            auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
            auto struct_type = struct_builder->type();
            PARQUET_THROW_NOT_OK(struct_builder->Append());
            for(size_t ifield = 0; ifield < struct_builder->num_children(); ifield++) {
                auto field_builder = struct_builder->child_builder(ifield);
                auto field_type = field_builder->type();
                auto field_name = struct_type->field(ifield)->ToString();

                types::buffer_t field_data = data_buffer.at(ifield);
                if(auto val = std::get_if<types::buffer_value_t>(&field_data)) {
                    std::stringstream field_node_name;
                    field_node_name << field_path << "." << field_name;
                    this->fill(field_node_name.str(), {*val});
                }
            } // ifield
        } // is_struct
    } else {
        auto data = data_buffer.at(0);
        if(auto val = std::get_if<types::buffer_value_t>(&data)) {
            // data is flat (array-like)
            if(auto v = std::get_if<bool>(val)) {
                helpers::fill<bool>(*v, builder);
            } else
            if(auto v = std::get_if<uint8_t>(val)) {
                helpers::fill<uint8_t>(*v, builder);
            } else
            if(auto v = std::get_if<uint16_t>(val)) {
                helpers::fill<uint16_t>(*v, builder);
            } else
            if(auto v = std::get_if<uint32_t>(val)) {
                helpers::fill<uint32_t>(*v, builder);
            } else
            if(auto v = std::get_if<uint64_t>(val)) {
                helpers::fill<uint64_t>(*v, builder);
            } else
            if(auto v = std::get_if<int8_t>(val)) {
                helpers::fill<int8_t>(*v, builder);
            } else
            if(auto v = std::get_if<int16_t>(val)) {
                helpers::fill<int16_t>(*v, builder);
            } else
            if(auto v = std::get_if<int32_t>(val)) {
                helpers::fill<int32_t>(*v, builder);
            } else
            if(auto v = std::get_if<int64_t>(val)) {
                helpers::fill<int64_t>(*v, builder);
            } else
            if(auto v = std::get_if<float>(val)) {
                helpers::fill<float>(*v, builder);
            } else
            if(auto v = std::get_if<double>(val)) {
                helpers::fill<double>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<bool>>(val)) {
                helpers::fill<std::vector<bool>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<uint8_t>>(val)) {
                helpers::fill<std::vector<uint8_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<uint16_t>>(val)) {
                helpers::fill<std::vector<uint16_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<uint32_t>>(val)) {
                helpers::fill<std::vector<uint32_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<uint64_t>>(val)) {
                helpers::fill<std::vector<uint64_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<int8_t>>(val)) {
                helpers::fill<std::vector<int8_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<int16_t>>(val)) {
                helpers::fill<std::vector<int16_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<int32_t>>(val)) {
                helpers::fill<std::vector<int32_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<int64_t>>(val)) {
                helpers::fill<std::vector<int64_t>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<float>>(val)) {
                helpers::fill<std::vector<float>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<double>>(val)) {
                helpers::fill<std::vector<double>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<bool>>>(val)) {
                helpers::fill<std::vector<std::vector<bool>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<uint8_t>>>(val)) {
                helpers::fill<std::vector<std::vector<uint8_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<uint16_t>>>(val)) {
                helpers::fill<std::vector<std::vector<uint16_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<uint32_t>>>(val)) {
                helpers::fill<std::vector<std::vector<uint32_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<uint64_t>>>(val)) {
                helpers::fill<std::vector<std::vector<uint64_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<int8_t>>>(val)) {
                helpers::fill<std::vector<std::vector<int8_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<int16_t>>>(val)) {
                helpers::fill<std::vector<std::vector<int16_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<int32_t>>>(val)) {
                helpers::fill<std::vector<std::vector<int32_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<int64_t>>>(val)) {
                helpers::fill<std::vector<std::vector<int64_t>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<float>>>(val)) {
                helpers::fill<std::vector<std::vector<float>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<double>>>(val)) {
                helpers::fill<std::vector<std::vector<double>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<bool>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<bool>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<uint8_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint8_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<uint16_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint16_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<uint32_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint32_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<uint64_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<uint64_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<int8_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<int8_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<int16_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<int16_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<int32_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<int32_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<int64_t>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<int64_t>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<float>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<float>>>>(*v, builder);
            } else
            if(auto v = std::get_if<std::vector<std::vector<std::vector<double>>>>(val)) {
                helpers::fill<std::vector<std::vector<std::vector<double>>>>(*v, builder);
            } else {
                throw std::logic_error("ERROR: Invalid type, cannot fill");
            }
        } else
        if(auto val = std::get_if<std::vector<types::buffer_value_t>>(&data)) {
            // here we handle the case of a data buffer containing a a
            // vector of potentially differently-typed fields (e.g. a struct)

            size_t try_count = 0;
            while(builder_type->id() != arrow::Type::STRUCT) {
                if(try_count > 3) {
                    std::stringstream err;
                    err << "ERROR: Expected builder type of \"struct\" for field with name \"" << field_path << "\", got \"" << builder_type->name() << "\"";
                    throw std::runtime_error(err.str());
                }
                if(builder_type->id() == arrow::Type::LIST) {
                    auto tmp = dynamic_cast<arrow::ListBuilder*>(builder)->value_builder();
                    auto tmp_type = tmp->type();
                    //if(tmp_type->id() == arrow::Type::STRUCT) {
                        builder = tmp;
                        builder_type = builder->type();
                    //}
                }
                try_count++;
            }
            types::buffer_value_vec_t field_data_vec = *val;

            auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
            auto struct_type = struct_builder->type();
            PARQUET_THROW_NOT_OK(struct_builder->Append());

            for(size_t ifield = 0; ifield < struct_builder->num_children(); ifield++) {
                auto field_builder = struct_builder->child_builder(ifield);
                auto field_type = field_builder->type();
                auto field_name = struct_type->field(ifield)->ToString();

                // don't consider inner structs, they must be called
                // manually with a new call to "fill"

                // for structs within structs, you MUST fill from outer to inner
                // e.g.
                //      writer.fill("struct", {struct_data});
                //      writer.fill("struct.inner_struct", {inner_struct_data});
                if(ifield >= field_data_vec.size()) break;
                if(field_type->id() == arrow::Type::STRUCT) { ifield--; continue; }

                bool field_ok = false;
                types::buffer_value_t field_data = field_data_vec.at(ifield);
                switch (field_type->id()) {
                    case arrow::Type::BOOL:
                        if(auto v = std::get_if<bool>(&field_data)) {
                            helpers::fill<bool>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT8:
                        if(auto v = std::get_if<uint8_t>(&field_data)) {
                            helpers::fill<uint8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT16:
                        if(auto v = std::get_if<uint16_t>(&field_data)) {
                            helpers::fill<uint16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT32:
                        if(auto v = std::get_if<uint32_t>(&field_data)) {
                            helpers::fill<uint32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::UINT64:
                        if(auto v = std::get_if<uint64_t>(&field_data)) {
                            helpers::fill<uint64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT8:
                        if(auto v = std::get_if<int8_t>(&field_data)) {
                            helpers::fill<int8_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT16:
                        if(auto v = std::get_if<int16_t>(&field_data)) {
                            helpers::fill<int16_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT32:
                        if(auto v = std::get_if<int32_t>(&field_data)) {
                            helpers::fill<int32_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                    case arrow::Type::INT64:
                        if(auto v = std::get_if<int64_t>(&field_data)) {
                            helpers::fill<int64_t>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                     case arrow::Type::FLOAT:
                        if(auto v = std::get_if<float>(&field_data)) {
                            helpers::fill<float>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;
                     case arrow::Type::DOUBLE:
                        if(auto v = std::get_if<double>(&field_data)) {
                            helpers::fill<double>(*v, field_builder.get());
                            field_ok = true;
                        }
                        break;

                     case arrow::Type::LIST:
                        if(auto v = std::get_if<std::vector<bool>>(&field_data)) {
                            helpers::fill<std::vector<bool>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<uint8_t>>(&field_data)) {
                            helpers::fill<std::vector<uint8_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<uint16_t>>(&field_data)) {
                            helpers::fill<std::vector<uint16_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<uint32_t>>(&field_data)) {
                            helpers::fill<std::vector<uint32_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<uint64_t>>(&field_data)) {
                            helpers::fill<std::vector<uint64_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<int8_t>>(&field_data)) {
                            helpers::fill<std::vector<int8_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<int16_t>>(&field_data)) {
                            helpers::fill<std::vector<int16_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<int32_t>>(&field_data)) {
                            helpers::fill<std::vector<int32_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<int64_t>>(&field_data)) {
                            helpers::fill<std::vector<int64_t>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<float>>(&field_data)) {
                            helpers::fill<std::vector<float>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<double>>(&field_data)) {
                            helpers::fill<std::vector<double>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        // 2D list field
                        if(auto v = std::get_if<std::vector<std::vector<bool>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<bool>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<uint8_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<uint8_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<uint16_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<uint16_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<uint32_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<uint32_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<uint64_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<uint64_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<int8_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<int8_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<int16_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<int16_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<int32_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<int32_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<int64_t>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<int64_t>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<float>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<float>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<double>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<double>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<bool>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<bool>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<uint8_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<uint8_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<uint16_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<uint16_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<uint32_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<uint32_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<uint64_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<uint64_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<int8_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<int8_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<int16_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<int16_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<int32_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<int32_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<int64_t>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<int64_t>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<float>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<float>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else
                        if(auto v = std::get_if<std::vector<std::vector<std::vector<double>>>>(&field_data)) {
                            helpers::fill<std::vector<std::vector<std::vector<double>>>>(*v, field_builder.get());
                            field_ok = true;
                        } else {
                            throw std::logic_error("ERROR Unhandled fill type for struct field");
                        }
                        break;
                    default:
                        throw std::logic_error("ERROR Could not fill field \"" + field_name + "\" for struct \"" + field_path);
                        break;
                } // switch
                if(!field_ok) {
                    throw std::runtime_error("ERROR Failed to fill field \"" + field_name + "\" for struct \"" + field_path);
                }
            } // ifield
        } else {
            std::stringstream err;
            err << "ERROR: Invalid data type given to fill method for field at \"" << field_path << "\"";
            throw std::runtime_error(err.str());
        }
    }


    _field_fill_count++;
    if(_field_fill_count % _fields.size() == 0) {
        _fill_count++;
        _row_length++;

        if(_fill_count % _n_rows_in_group == 0) {
            this->flush();
        }
    }
}

void Writer::flush() {

    _arrays.clear();
    std::shared_ptr<arrow::Array> array;
    for(auto& field_name : _fields) {
        PARQUET_THROW_NOT_OK(_col_builder_map.at(field_name->name()).at(field_name->name())->Finish(&array));
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

}; // namespace parquetwriter
