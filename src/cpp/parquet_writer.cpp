#include "parquet_writer.h"

#include "parquet_writer_exceptions.h"
#include "parquet_writer_helpers.h"
#include "parquet_writer_visitor.h"

// std/stl
#include <algorithm>
#include <filesystem>
#include <sstream>

namespace parquetwriter {

Writer::Writer()
    : _output_directory("./"),
      _dataset_name(""),
      _file_count(0),
      _n_rows_in_group(-1),
      _n_current_rows_filled(0),
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
    infile.seekg(0);
    nlohmann::json jlayout;
    try {
        jlayout = nlohmann::json::parse(infile);
    } catch (std::exception& e) {
        throw std::runtime_error("Failed to parse input field layout JSON: " +
                                 std::string(e.what()));
    }
    this->set_layout(jlayout);
}

void Writer::set_layout(const std::string& field_layout_json_str) {
    nlohmann::json jlayout;
    try {
        jlayout = nlohmann::json::parse(field_layout_json_str);
    } catch (std::exception& e) {
        throw std::runtime_error("Failed to parse input field layout JSON: " +
                                 std::string(e.what()));
    }
    this->set_layout(jlayout);
}

void Writer::set_layout(const nlohmann::json& field_layout) {
    // there must be a top-level "fields" node

    _columns = helpers::columns_from_json(field_layout);
    if (_columns.size() == 0) {
        throw parquetwriter::layout_exception(
            "No fields constructed from provided layout");
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
        throw parquetwriter::layout_exception(
            "Failed to parse input metadata JSON: " + std::string(e.what()));
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
        throw parquetwriter::layout_exception(
            "Failed to parse input metadata JSON: " + std::string(e.what()));
    }

    if (_schema) {
        this->set_metadata(_file_metadata);
    }
}

void Writer::set_metadata(const nlohmann::json& metadata) {
    _file_metadata = metadata;
    if (_file_metadata.count("metadata") == 0) {
        throw parquetwriter::layout_exception(
            "Provided metadata JSON is missing top-level \"metadata\" node");
    }

    if (_schema) {
        std::unordered_map<std::string, std::string> metadata_map;
        metadata_map["metadata"] = metadata["metadata"].dump();
        arrow::KeyValueMetadata keyval_metadata(metadata_map);
        _schema = _schema->WithMetadata(keyval_metadata.Copy());
    }
}

void Writer::set_dataset_name(const std::string& dataset_name) {
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
        throw parquetwriter::writer_exception("Empty dataset name");
    }

    if (!_schema) {
        throw parquetwriter::writer_exception("Empty Parquet schema");
    }

    if (_columns.size() == 0) {
        throw parquetwriter::writer_exception("Empty file layout (no columns)");
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
        throw parquetwriter::not_implemented_exception(
            "FlushRule::BUFFERSIZE not supporterd");
    }
    _flush_rule = rule;
    _n_rows_in_group = n;
}

void Writer::fill_value(const std::string& field_name,
                        arrow::ArrayBuilder* builder,
                        const std::vector<value_t>& data_buffer) {
    if (data_buffer.size() != 1) {
        throw parquetwriter::data_buffer_exception(
            "Invalid data buffer shape for column/field \"" + field_name +
            "\": expects data buffer size: 1, got: " +
            std::to_string(data_buffer.size()));
    }

    auto data = data_buffer.at(0);
    if (auto val = std::get_if<value_t>(&data)) {
        std::visit(internal::DataValueFillVisitor(field_name, builder), *val);
    } else {
        throw parquetwriter::data_buffer_exception(
            "Invalid variant type encountered");
    }
}

void Writer::fill_value_list(const std::string& field_name,
                             arrow::ArrayBuilder* builder,
                             const std::vector<value_t>& data_buffer) {
    if (data_buffer.size() != 1) {
        throw parquetwriter::data_buffer_exception(
            "Invalid data buffer shape for column/field \"" + field_name +
            "\": expects data buffer size: 1, got: " +
            std::to_string(data_buffer.size()));
    }

    auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
    this->fill_value(field_name, list_builder, data_buffer);
}

void Writer::fill_struct_list_(
    const std::string& field_name, arrow::ArrayBuilder* builder,
    const std::vector<value_t>& data_buffer) {
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
    auto [depth, terminal_builder] =
        helpers::list_builder_description(list_builder);

    if (depth == 1) {
        // initiate a new list element
        PARQUET_THROW_NOT_OK(list_builder->Append());

        // initiate a new struct element
        auto value_builder = list_builder->value_builder();
        for (size_t i = 0; i < data_buffer.size(); i++) {
            field_buffer_t struct_data =
                helpers::struct_from_data_buffer_element(data_buffer.at(i),
                                                         field_name);
            this->fill_struct_(field_name, value_builder, {struct_data});
        }
    } else if (depth == 2) {
        PARQUET_THROW_NOT_OK(list_builder->Append());
        list_builder =
            dynamic_cast<arrow::ListBuilder*>(list_builder->value_builder());
        auto value_builder = list_builder->value_builder();
        for (size_t i = 0; i < data_buffer.size(); i++) {
            PARQUET_THROW_NOT_OK(list_builder->Append());
            auto inner_data =
                std::get<std::vector<value_t>>(data_buffer.at(i));
            for (size_t j = 0; j < inner_data.size(); j++) {
                field_buffer_t struct_data =
                    helpers::struct_from_data_buffer_element(inner_data.at(j),
                                                             field_name);
                this->fill_struct_(field_name, value_builder, {struct_data});
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
                std::get<std::vector<std::vector<field_buffer_t>>>(
                    data_buffer.at(i));
            for (size_t j = 0; j < inner_data.size(); j++) {
                PARQUET_THROW_NOT_OK(inner_inner_list_builder->Append());
                auto inner_inner_data = inner_data.at(
                    j);  // std::get<std::vector<field_buffer_t>>(inner_data.at(j));
                for (size_t k = 0; k < inner_inner_data.size(); k++) {
                    field_buffer_t struct_data =
                        helpers::struct_from_data_buffer_element(
                            inner_inner_data.at(k), field_name);
                    this->fill_struct_(field_name, value_builder,
                                       {struct_data});
                }  // k
            }      // j
        }          // i
    }
}

void Writer::fill_struct_(const std::string& path_name,
                          arrow::ArrayBuilder* builder,
                          const std::vector<value_t>& data_buffer) {
    //
    // get the struct data
    //
    field_buffer_t struct_data =
        helpers::struct_from_data_buffer_element(data_buffer.at(0), path_name);

    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
    auto [num_total_fields, num_fields_nonstruct] =
        helpers::field_nums_from_struct(struct_builder, path_name);

    if (struct_data.size() != num_fields_nonstruct) {
        throw parquetwriter::data_buffer_exception(
            "Invalid number of data elements provided for struct column/field "
            "\"" +
            path_name + "\", expect: " + std::to_string(num_fields_nonstruct) +
            ", got: " + std::to_string(struct_data.size()));
    }

    // initiate a new struct element
    PARQUET_THROW_NOT_OK(struct_builder->Append());

    //
    // here will fill the fields of the struct, assuming that the order and type
    // of the data in the provided data_buffer/field_buffer_t matches that of
    // the actual column type
    //
    auto field_buffer_type = struct_builder->type();
    size_t data_idx = 0;
    for (size_t ifield = 0; ifield < num_total_fields; ifield++) {
        auto field_builder = struct_builder->child_builder(ifield).get();
        auto field_type = field_builder->type();
        auto field_name = field_buffer_type->field(ifield)->name();

        // presumably we have finished filling all the requisite fields
        if (data_idx >= struct_data.size()) {
            break;
        }

        // skip struct-typed and struct_list-typed fields
        if (helpers::builder_is_struct_type(field_builder)) {
            continue;
        }

        std::stringstream field_path_name;
        field_path_name << path_name << "." << field_name;
        this->fill_value(field_path_name.str(), field_builder,
                         {struct_data.at(data_idx)});
        data_idx++;
    }  // ifield
}

void Writer::fill(const std::string& field_path,
                  const std::vector<value_t>& data_buffer) {
    if (_expected_fields_filltype_map.count(field_path) == 0) {
        throw parquetwriter::writer_exception(
            "Cannot fill unknown column/field \"" + field_path + "\"");
    }

    //
    // get the parent column name (needed for cases in which this is a
    // sub-field)
    //
    size_t pos_parent = field_path.find_first_of(".");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }
    if (_column_builder_map.count(parent_column_name) == 0) {
        throw parquetwriter::writer_exception(
            "Parent column associated with column/field \"" + field_path +
            "\" could not be found");
    }

    auto builder = _column_builder_map.at(parent_column_name).at(field_path);
    if (!builder) {
        throw parquetwriter::writer_exception(
            "ArrayBuilder for column/field \"" + field_path + "\" is null");
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
            throw parquetwriter::data_buffer_exception(
                "Empty data buffer provided for column/field \"" + field_path +
                "\" that is not of list-type");
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
                this->fill_struct_(field_path, builder, data_buffer);
                break;
            case FillType::STRUCT_LIST_1D:
            case FillType::STRUCT_LIST_2D:
            case FillType::STRUCT_LIST_3D:
                this->fill_struct_list_(field_path, builder, data_buffer);
                break;
            default:
                throw parquetwriter::writer_exception(
                    "Invalid FillType \"" +
                    filltype_to_string(field_fill_type) + "\"");
                break;
        }  // switch
    }

    // signal that this column/field was succesfully filled
    increment_field_fill_count(field_path);

    // check & signal that the row is complete
    check_row_complete();
}

field_buffer_t Writer::to_struct(const std::string& field_path,
                                 const struct_t& field_map) {
    std::vector<std::string> ordered_fields =
        this->struct_fill_order(field_path);
    field_buffer_t ordered_struct_data;
    for (const auto& expected_field_name : ordered_fields) {
        if (field_map.count(expected_field_name) == 0) {
            throw parquetwriter::data_type_exception(
                "Provided field map for struct column/field \"" + field_path +
                "\" is missing data for expected field \"" +
                expected_field_name + "\"");
        }
        ordered_struct_data.push_back(field_map.at(expected_field_name));
    }
    return ordered_struct_data;
}

void Writer::fill_struct(
    const std::string& field_path,
    const std::map<std::string, value_t>& struct_field_map) {
    auto ordered_struct_data = this->to_struct(field_path, struct_field_map);
    this->fill(field_path, {ordered_struct_data});
}

void Writer::fill_struct_list(
    const std::string& field_path,
    const std::vector<std::map<std::string, value_t>>& struct_field_map_vec) {
    struct_list1d struct_list_data;
    for (size_t i = 0; i < struct_field_map_vec.size(); i++) {
        auto ordered_struct_data =
            this->to_struct(field_path, struct_field_map_vec.at(i));
        struct_list_data.push_back(ordered_struct_data);
    }  // i
    this->fill(field_path, {struct_list_data});
}

void Writer::fill_struct_list(
    const std::string& field_path,
    const std::vector<std::vector<std::map<std::string, value_t>>>&
        struct_field_map_vec) {
    struct_list2d struct_list_data;
    for (size_t i = 0; i < struct_field_map_vec.size(); i++) {
        std::vector<field_buffer_t> inner_data;
        for (size_t j = 0; j < struct_field_map_vec.at(i).size(); j++) {
            auto ordered_struct_data =
                this->to_struct(field_path, struct_field_map_vec.at(i).at(j));
            inner_data.push_back(ordered_struct_data);
        }  // j
        struct_list_data.push_back(inner_data);
    }  // i
    this->fill(field_path, {struct_list_data});
}

void Writer::fill_struct_list(
    const std::string& field_path,
    const std::vector<std::vector<std::vector<std::map<std::string, value_t>>>>&
        struct_field_map_vec) {
    struct_list3d struct_list_data;
    for (size_t i = 0; i < struct_field_map_vec.size(); i++) {
        std::vector<std::vector<field_buffer_t>> inner_data;
        for (size_t j = 0; j < struct_field_map_vec.at(i).size(); j++) {
            std::vector<field_buffer_t> inner_inner_data;
            for (size_t k = 0; k < struct_field_map_vec.at(i).at(j).size();
                 k++) {
                auto ordered_struct_data = this->to_struct(
                    field_path, struct_field_map_vec.at(i).at(j).at(k));
                inner_inner_data.push_back(ordered_struct_data);
            }  // k
            inner_data.push_back(inner_inner_data);
        }  // j
        struct_list_data.push_back(inner_data);
    }  // i
    this->fill(field_path, {struct_list_data});
}

std::vector<std::string> Writer::struct_fill_order(
    const std::string& field_path) {
    if (_expected_fields_filltype_map.count(field_path) == 0) {
        throw parquetwriter::writer_exception(
            "Cannot fill unknown column/field \"" + field_path + "\"");
    }

    size_t pos_parent = field_path.find_first_of(".");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    if (_column_builder_map.count(parent_column_name) == 0) {
        throw parquetwriter::writer_exception(
            "Parent column associated with column/field \"" + field_path +
            "\" could not be found");
    }

    auto builder = _column_builder_map.at(parent_column_name).at(field_path);
    if (!builder) {
        throw parquetwriter::writer_exception(
            "ArrayBuilder for column/field \"" + field_path + "\" is null");
    }

    std::vector<std::string> field_ordering =
        helpers::struct_field_order_from_builder(builder, field_path);

    if (field_ordering.size() == 0) {
        throw parquetwriter::writer_exception(
            "No fields found for expected struct builder column/field \"" +
            field_path + "\"");
    }
    return field_ordering;
}

void Writer::increment_field_fill_count(const std::string& field_path) {
    if (_expected_field_fill_map.count(field_path) == 0) {
        throw parquetwriter::writer_exception(
            "Unexpected column/field encountered \"" + field_path + "\"");
    }
    _expected_field_fill_map.at(field_path)++;
}

void Writer::check_row_complete() {
    if (row_is_complete()) {
        _n_current_rows_filled++;
        flush_if_ready();
    }
}

bool Writer::row_is_complete() {
    // check that each expected fill field has been filled once
    for (const auto& [field_name, field_fill_count] :
         _expected_field_fill_map) {
        if (field_fill_count != 1) {
            return false;
        }
    }
    return true;
}

void Writer::flush_if_ready() {
    if (_n_current_rows_filled % _n_rows_in_group == 0) {
        this->flush();
    }
}

void Writer::append_empty_value(const std::string& field_path) {
    arrow::ArrayBuilder* builder = nullptr;
    size_t pos_parent = field_path.find_first_of("/.");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }

    auto parent_count = _expected_field_fill_map.at(parent_column_name);
    for (const auto& [sub_field_name, sub_field_count] :
         _expected_field_fill_map) {
        if (sub_field_name == parent_column_name) continue;
        std::stringstream sub_find;
        sub_find << parent_column_name << ".";
        bool is_sub_field_of_parent =
            sub_field_name.find(sub_find.str()) != std::string::npos;
        if (is_sub_field_of_parent) {
            // this is a sub-field of the current parent field
            if (!(parent_count >= sub_field_count)) {
                throw parquetwriter::writer_exception(
                    "Cannot append empty value to column/field \"" +
                    field_path + "\": parent column/field (\"" +
                    parent_column_name +
                    "\") fill count < child column/field (\"" + field_path +
                    "\") fill count (" + std::to_string(parent_count) +
                    " != " + std::to_string(sub_field_count) + ")");
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

    // we should not append a null value to a sub-field if the
    // counts differ between parent and child column since then
    // the offsets will likely be incorrect
    auto parent_count = _expected_field_fill_map.at(parent_column_name);
    for (const auto& [sub_field_name, sub_field_count] :
         _expected_field_fill_map) {
        if (sub_field_name == parent_column_name) continue;
        std::stringstream sub_find;
        sub_find << parent_column_name << ".";
        bool is_sub_field_of_parent =
            sub_field_name.find(sub_find.str()) != std::string::npos;
        if (is_sub_field_of_parent) {
            // this is a sub-field of the current parent field
            if (sub_field_count != parent_count) {
                throw parquetwriter::writer_exception(
                    "Cannot append null to column/field \"" + field_path +
                    "\": parent column/field (\"" + parent_column_name +
                    "\") fill count != child column/field (\"" + field_path +
                    "\") fill count (" + std::to_string(parent_count) +
                    " != " + std::to_string(sub_field_count) + ")");
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
    bool corrected_offset = false;
    for (const auto& field : _expected_fields_to_fill) {
        auto field_fill_count = _expected_field_fill_map.at(field);

        if (field_fill_count == 0) {
            // append null to columns that did not have "fill" called on them
            corrected_offset = true;
            this->append_null_value(field);
            increment_field_fill_count(field);
        } else if (field_fill_count > 1) {
            throw parquetwriter::writer_exception(
                "Column/field \"" + field +
                "\" has been filled too many times for a single row (expected "
                "fill count: 1, got: " +
                std::to_string(field_fill_count) + ")");
        }
    }
    if (corrected_offset) {
        check_row_complete();
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
    PARQUET_THROW_NOT_OK(
        _file_writer->WriteTable(*table, _n_current_rows_filled));
    _n_current_rows_filled = 0;

    // flush to the output file
    PARQUET_THROW_NOT_OK(_output_stream->Flush());
    _arrays.clear();
}

void Writer::finish() {
    this->flush();
    PARQUET_THROW_NOT_OK(_file_writer->Close());
}

};  // namespace parquetwriter
