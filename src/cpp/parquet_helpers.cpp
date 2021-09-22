#include "parquet_helpers.h"

#include "logging.h"

// std/stl
#include <sstream>
namespace parquetwriter {
namespace helpers {

std::shared_ptr<arrow::DataType> datatype_from_string(
    const std::string& type_string) {
    std::shared_ptr<arrow::DataType> out_type = nullptr;
    if (internal::type_init_map.count(type_string) == 0) {
        std::stringstream err;
        err << "ERROR: Unsupported type string provided: \"" << type_string
            << "\"";
        throw std::runtime_error(err.str());
    }
    internal::ArrowTypeInit TypeInit;
    return (TypeInit.*(internal::type_init_map.at(type_string)))();
}

std::vector<std::shared_ptr<arrow::Field>> columns_from_json(
    const json& jlayout, const std::string& current_node) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    nlohmann::json jfields;
    try {
        jfields = jlayout.at("fields");
    } catch (std::exception& e) {
        std::stringstream err;
        err << "ERROR: Missing required \"fields\" node in JSON layout";
        if (!current_node.empty()) {
            err << " (at node \"" << current_node << "\")";
        }
        logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                     err.str());
        throw std::runtime_error(e.what());
    }

    size_t n_fields = jfields.size();
    for (size_t ifield = 0; ifield < n_fields; ifield++) {
        auto jfield = jfields.at(ifield);
        auto field_name = jfield.at("name").get<std::string>();
        auto field_type = jfield.at("type");
        if (field_type.is_string()) {
            //
            // list
            //
            if (field_type == "list") {
                auto jcontains = jfield.at("contains");
                auto value_type = jcontains.at("type").get<std::string>();

                // list of lists
                if (value_type == "list") {
                    auto jcontains2 = jcontains.at("contains");
                    auto value_type2 = jcontains2.at("type").get<std::string>();

                    // list of list of lists (and that's it!)
                    if (value_type2 == "list") {
                        auto jcontains3 = jcontains2.at("contains");
                        auto value_type3 =
                            jcontains3.at("type").get<std::string>();

                        if (value_type3 == "list") {
                            std::stringstream err;
                            err << "ERROR: Invalid list depth (>3) encountered "
                                   "for field with name \""
                                << field_name << "\"";
                            throw std::runtime_error(err.str());
                        } else if (value_type3 == "struct") {
                            auto struct_fields =
                                columns_from_json(jcontains3, field_name);
                            auto end_list_type = arrow::struct_(struct_fields);
                            fields.push_back(arrow::field(
                                field_name, arrow::list(arrow::list(
                                                arrow::list(end_list_type)))));
                        } else {
                            fields.push_back(arrow::field(
                                field_name,
                                arrow::list(arrow::list(arrow::list(
                                    datatype_from_string(value_type3))))));
                        }
                    } else if (value_type2 == "struct") {
                        auto struct_fields =
                            columns_from_json(jcontains2, field_name);
                        auto end_list_type = arrow::struct_(struct_fields);
                        fields.push_back(arrow::field(
                            field_name,
                            arrow::list(arrow::list(end_list_type))));
                    } else {
                        fields.push_back(arrow::field(
                            field_name,
                            arrow::list(arrow::list(
                                datatype_from_string(value_type2)))));
                    }
                } else if (value_type == "struct") {
                    auto struct_fields =
                        columns_from_json(jcontains, field_name);
                    auto end_list_type = arrow::struct_(struct_fields);
                    fields.push_back(
                        arrow::field(field_name, arrow::list(end_list_type)));
                } else {
                    fields.push_back(arrow::field(
                        field_name,
                        arrow::list(datatype_from_string(value_type))));
                }
            }  // list
            else if (field_type == "struct") {
                auto struct_fields = columns_from_json(jfield, field_name);
                fields.push_back(
                    arrow::field(field_name, arrow::struct_(struct_fields)));
            } else {
                fields.push_back(
                    arrow::field(field_name, datatype_from_string(field_type)));
            }
        }
    }  // ifield
    return fields;
}

std::pair<std::vector<std::string>,
          std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>>
fill_field_builder_map_from_columns(
    const std::vector<std::shared_ptr<arrow::Field>>& columns) {
    auto log = logging::get_logger();

    //
    // we want to create an entry for each column and then
    // a sub-map for each builder associated with a call
    // to parquetwriter::Writer::fill
    //

    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> out;
    std::vector<std::string> field_names;

    for (auto column : columns) {
        auto column_name = column->name();
        auto column_type = column->type();

        auto pool = arrow::default_memory_pool();
        std::unique_ptr<arrow::ArrayBuilder> tmp;
        PARQUET_THROW_NOT_OK(arrow::MakeBuilder(pool, column_type, &tmp));

        // this is the top-level ArrayBuilder for this column,
        // all other builders for any sub-arrays (e.g. list or struct types)
        // can be inferred from it
        auto column_builder = tmp.release();

        // create the map to all builders for this column
        std::map<std::string, arrow::ArrayBuilder*> column_builders;

        // add the parent builder -- this is all that is needed for a column
        // that is filling only FillTypes::VALUE
        column_builders[column_name] = column_builder;
        field_names.push_back(column_name);

        // keep track of the fill type for this column
        // parquetwriter::FillType fill_type = parquetwriter::FillType::VALUE;

        parquetwriter::FillType column_fill_type =
            column_filltype_from_builder(column_builder, column_name);

        if (column_fill_type == parquetwriter::FillType::INVALID) {
            throw std::runtime_error("Bad column \"" + column_name + "\"");
        }

        // get the names of any sub-struct typed fields of structures
        if (column_fill_type == parquetwriter::FillType::STRUCT) {
            auto [names, struct_type_builders] =
                struct_type_field_builders(column_builder, column_name);
            for (size_t i = 0; i < struct_type_builders.size(); i++) {
                std::stringstream sub_name;
                sub_name << column_name << "." << names.at(i);
                column_builders[sub_name.str()] = struct_type_builders.at(i);
                field_names.push_back(sub_name.str());
            }
        }

        out[column_name] = column_builders;
    }  // column iterator
    return std::make_pair(field_names, out);
}

parquetwriter::struct_t struct_from_data_buffer_element(
    const parquetwriter::types::buffer_t& data, const std::string& field_name) {
    struct_t struct_data;
    try {
        struct_data = std::get<struct_t>(data);
    } catch (std::exception& e) {
        std::stringstream err;
        err << "Unable to parse struct field data for field \"" << field_name
            << "\"";
        logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                     err.str());
        throw std::runtime_error("Invalid data shape");
    }
    return struct_data;
}

std::pair<std::vector<std::string>, std::vector<arrow::ArrayBuilder*>>
struct_type_field_builders(arrow::ArrayBuilder* builder,
                           const std::string& column_name) {
    if (builder->type()->id() != arrow::Type::STRUCT) {
        std::stringstream err;
        err << "Provided builder (name = \"" << column_name
            << "\") is not of STRUCT type, has type \""
            << builder->type()->name() << "\"";
        logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                     err.str());
        throw std::logic_error("Invalid builder type");
    }
    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);

    std::vector<arrow::ArrayBuilder*> out;
    std::vector<std::string> names;
    unsigned n_fields = struct_builder->num_children();
    for (size_t ifield = 0; ifield < n_fields; ifield++) {
        auto field_builder = struct_builder->child_builder(ifield).get();
        auto field_type = field_builder->type();
        std::string field_name = struct_builder->type()->field(ifield)->name();
        bool is_list = field_type->id() == arrow::Type::LIST;
        bool is_struct = field_type->id() == arrow::Type::STRUCT;

        if (is_list) {
            // check if struct_list
            auto list_builder =
                dynamic_cast<arrow::ListBuilder*>(field_builder);
            auto [depth, terminal_builder] =
                list_builder_description(list_builder);
            if (terminal_builder->type()->id() == arrow::Type::STRUCT) {
                names.push_back(field_name);
                out.push_back(field_builder);
            }
        } else if (is_struct) {
            names.push_back(field_name);
            out.push_back(field_builder);
        }
    }  // ifield
    return std::make_pair(names, out);
}

std::pair<unsigned, arrow::ArrayBuilder*> list_builder_description(
    arrow::ListBuilder* builder) {
    unsigned depth = 1;
    auto list_builder = builder;
    auto value_builder = list_builder->value_builder();

    size_t unpack_count = 0;
    while (value_builder->type()->id() == arrow::Type::LIST) {
        if (unpack_count >= 3) break;
        depth++;
        list_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
        value_builder = list_builder->value_builder();
        unpack_count++;
    }
    return std::make_pair(depth, value_builder);
}

parquetwriter::FillType column_filltype_from_builder(
    arrow::ArrayBuilder* column_builder, const std::string& column_name) {
    auto log = logging::get_logger();
    // std::string column_name = column_builder->name();

    //
    // if there are any nested data structures, get the associated builders
    //
    bool is_list = column_builder->type()->id() == arrow::Type::LIST;
    bool is_struct = column_builder->type()->id() == arrow::Type::STRUCT;

    // For FillTypes::VALUE_LIST_{1D,2D,3D} we do not need entries
    // for the value_builders since they can always be inferred from the
    // top level

    if (is_list) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(column_builder);
        auto [depth, terminal_builder] = list_builder_description(list_builder);

        if (terminal_builder->type()->id() == arrow::Type::LIST) {
            std::stringstream err;
            err << "List depth >3 not supported, bad column \"" << column_name
                << "\"";
            logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                         err.str());
            return parquetwriter::FillType::INVALID;
        }

        //
        // either a struct_list or value_list
        //
        if (terminal_builder->type()->id() == arrow::Type::STRUCT) {
            // enforce that the structs contained in a struct_list column
            // do not have fields that are themselves structs or struct-lists
            auto struct_builder =
                dynamic_cast<arrow::StructBuilder*>(terminal_builder);
            if (!struct_builder) {
                std::stringstream err;
                err << "FillType for STRUCT_LIST did not have terminal builder "
                       "of type STRUCT";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                throw std::logic_error("Bad column");
            }
            if (!valid_sub_struct_layout(struct_builder, column_name)) {
                std::stringstream err;
                err << "Invalid layout for column \"" << column_name << "\"";
                logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                             err.str());
                return parquetwriter::FillType::INVALID;
            }

            switch (depth) {
                case 1:
                    return parquetwriter::FillType::STRUCT_LIST_1D;
                    break;
                case 2:
                    return parquetwriter::FillType::STRUCT_LIST_2D;
                    break;
                case 3:
                    return parquetwriter::FillType::STRUCT_LIST_3D;
                    break;
                default:
                    return parquetwriter::FillType::INVALID;
                    break;
            }  // switch
        } else {
            switch (depth) {
                case 1:
                    return parquetwriter::FillType::VALUE_LIST_1D;
                    break;
                case 2:
                    return parquetwriter::FillType::VALUE_LIST_2D;
                    break;
                case 3:
                    return parquetwriter::FillType::VALUE_LIST_3D;
                    break;
                default:
                    return parquetwriter::FillType::INVALID;
                    break;
            }  // switch
        }
    } else if (is_struct) {
        auto struct_builder =
            dynamic_cast<arrow::StructBuilder*>(column_builder);
        unsigned number_of_fields = struct_builder->num_children();
        for (size_t ichild = 0; ichild < number_of_fields; ichild++) {
            auto child_builder = struct_builder->child_builder(ichild).get();
            std::string field_name =
                struct_builder->type()->field(ichild)->name();
            if (child_builder->type()->id() == arrow::Type::STRUCT) {
                // no sub-structs that have fields of type struct
                auto sub_struct_builder =
                    dynamic_cast<arrow::StructBuilder*>(child_builder);
                if (!valid_sub_struct_layout(sub_struct_builder, column_name)) {
                    std::stringstream err;
                    err << "Invalid layout for column \"" << column_name
                        << "\"";
                    logging::get_logger()->error("{0} - {1}",
                                                 __PRETTYFUNCTION__, err.str());
                    return parquetwriter::FillType::INVALID;
                }
            } else if (child_builder->type()->id() == arrow::Type::LIST) {
                // no sub-struct-lists
                auto sub_list_builder =
                    dynamic_cast<arrow::ListBuilder*>(child_builder);
                auto [sub_list_depth, sub_list_terminal_builder] =
                    list_builder_description(sub_list_builder);

                // check dimension
                if (sub_list_terminal_builder->type()->id() ==
                    arrow::Type::LIST) {
                    std::stringstream err;
                    err << "List depth >3 not supported for list field \""
                        << field_name << "\" of struct column \"" << column_name
                        << "\"";
                    logging::get_logger()->error("{0} - {1}",
                                                 __PRETTYFUNCTION__, err.str());
                    return parquetwriter::FillType::INVALID;
                } else if (sub_list_terminal_builder->type()->id() ==
                           arrow::Type::STRUCT) {
                    // check that sub-structs are "flat"
                    auto sub_struct_builder =
                        dynamic_cast<arrow::StructBuilder*>(
                            sub_list_terminal_builder);
                    if (!valid_sub_struct_layout(sub_struct_builder,
                                                 column_name)) {
                        std::stringstream err;
                        err << "Invalid layout for field \"" << field_name
                            << "\" of struct column \"" << column_name << "\"";
                        logging::get_logger()->error(
                            "{0} - {1}", __PRETTYFUNCTION__, err.str());
                        return parquetwriter::FillType::INVALID;
                    }
                }
            }
        }
        return parquetwriter::FillType::STRUCT;
    }  // is_struct
    return parquetwriter::FillType::VALUE;
}

bool valid_sub_struct_layout(arrow::StructBuilder* struct_builder,
                             const std::string& parent_column_name) {
    auto log = logging::get_logger();

    //
    // any struct type that is itself a child field of some nested type
    // cannot itself have any terminal fields that are structs: i.e.
    // struct lists and structs are not allowd
    //
    unsigned total_number_of_fields = struct_builder->num_children();
    for (size_t ichild = 0; ichild < total_number_of_fields; ichild++) {
        auto child_field_builder = struct_builder->child_builder(ichild).get();
        auto child_field_name = struct_builder->type()->field(ichild)->name();

        //
        // no sub struct allowed
        //
        if (child_field_builder->type()->id() == arrow::Type::STRUCT) {
            //
            // disallow a field if it itself is a struct
            //
            std::stringstream err;
            err << "Child struct of column \"" << parent_column_name
                << "\" has invalid field \"" << child_field_name
                << "\" with type \"struct\"";
            logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                         err.str());
            return false;
        }  // is_struct
        else if (child_field_builder->type()->id() == arrow::Type::LIST) {
            //
            // disallow a field if it itself is a struct list
            //

            size_t unpack_count = 0;
            auto list_builder =
                dynamic_cast<arrow::ListBuilder*>(child_field_builder);
            auto value_builder = list_builder->value_builder();
            while (value_builder->type()->id() == arrow::Type::LIST) {
                if (unpack_count >= 3) break;
                list_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
                value_builder = list_builder->value_builder();
                unpack_count++;
            }

            if (value_builder->type()->id() == arrow::Type::STRUCT) {
                std::stringstream err;
                err << "Child struct of column \"" << parent_column_name
                    << "\"  has invalid field \"" << child_field_name
                    << "\" with type \"struct list\"";
                logging::get_logger()->error("{0} - {1}", __PRETTYFUNCTION__,
                                             err.str());
                return false;
            }
        }  // is_list
    }      // ichild

    return true;
}

std::pair<unsigned, unsigned> field_nums_from_struct(
    const arrow::StructBuilder* builder, const std::string& column_name) {
    // cache
    static std::map<std::string, std::pair<unsigned, unsigned>> count_map;
    if (count_map.count(column_name) > 0) {
        return count_map.at(column_name);
    }

    unsigned total_num = builder->num_children();
    unsigned total_non_struct = 0;

    // loop over the child builders and find all those that are not of struct
    // type (either directly a struct or a list that terminally contains a
    // struct list)
    for (size_t i = 0; i < total_num; i++) {
        auto child = builder->child_builder(i).get();
        size_t try_count = 0;

        // unpack the list to see if it is a struct list
        if (child->type()->id() == arrow::Type::LIST) {
            auto list_builder = dynamic_cast<arrow::ListBuilder*>(child);
            auto value_builder = list_builder->value_builder();
            while (value_builder->type()->id() == arrow::Type::LIST) {
                if (try_count >= 3) break;
                list_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
                value_builder = list_builder->value_builder();
                try_count++;
            }
            child = value_builder;
        }
        if (child->type()->id() == arrow::Type::STRUCT) continue;
        total_non_struct++;
    }
    auto out = std::make_pair(total_num, total_non_struct);
    count_map[column_name] = out;
    return out;
}

};  // namespace helpers
};  // namespace parquetwriter
