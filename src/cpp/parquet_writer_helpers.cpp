#include "parquet_writer_helpers.h"

#include "parquet_writer_exceptions.h"

// std/stl
#include <sstream>

namespace parquetwriter {
namespace helpers {

std::shared_ptr<arrow::DataType> datatype_from_string(
    const std::string& type_string) {
    std::shared_ptr<arrow::DataType> out_type = nullptr;
    if (internal::type_init_map.count(type_string) == 0) {
        throw parquetwriter::layout_exception(
            "Unsupported type \"" + type_string + "\" specified in layout");
    }
    internal::ArrowTypeInit TypeInit;
    return (TypeInit.*(internal::type_init_map.at(type_string)))();
}

void check_layout_list(const nlohmann::json& list_layout,
                       const std::string& column_name) {
    if (!(list_layout.count("contains") > 0 &&
          list_layout.at("contains").is_object())) {
        throw parquetwriter::layout_exception(
            "Invalid JSON layout for list type column \"" + column_name + "\"");
    }

    if (!(list_layout.at("contains").count("type") > 0)) {
        throw parquetwriter::layout_exception(
            "\"contains\" object for list type column \"" + column_name +
            "\" is missing \"type\" specification");
    }
}

void check_layout_struct(const nlohmann::json& struct_layout,
                         const std::string& column_name) {
    if (!(struct_layout.count("fields") > 0 &&
          struct_layout.at("fields").is_array())) {
        throw parquetwriter::layout_exception(
            "Invalid JSON layout for struct type column \"" + column_name +
            "\"");
    }
}

std::vector<std::shared_ptr<arrow::Field>> columns_from_json(
    const json& jlayout, const std::string& current_node) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    nlohmann::json jfields;
    try {
        jfields = jlayout.at("fields");
    } catch (std::exception& e) {
        throw parquetwriter::layout_exception(
            "JSON layout for " +
            (current_node.empty() ? "top-level layout "
                                  : "\"" + current_node + "\" ") +
            "is missing required \"fields\" object");
    }

    size_t n_fields = jfields.size();
    for (size_t ifield = 0; ifield < n_fields; ifield++) {
        auto jfield = jfields.at(ifield);
        auto field_name = jfield.at("name").get<std::string>();
        auto field_type = jfield.at("type");

        if (field_type == "list1d" || field_type == "list2d" ||
            field_type == "list3d") {
            // list types
            check_layout_list(jfield, field_name);
            unsigned depth =
                (field_type == "list1d" ? 1 : (field_type == "list2d" ? 2 : 3));
            auto jcontains = jfield.at("contains");
            auto type_string = jcontains.at("type").get<std::string>();
            std::shared_ptr<arrow::DataType> list_type;
            if (type_string == "struct") {
                check_layout_struct(jcontains, field_name);
                // struct_list
                auto struct_fields = columns_from_json(jcontains, field_name);
                list_type = arrow::struct_(struct_fields);
            } else {
                // value_list
                list_type = datatype_from_string(type_string);
            }
            unsigned level = 0;
            while (level < depth) {
                level++;
                list_type = arrow::list(list_type);
            }
            fields.push_back(arrow::field(field_name, list_type));
        } else if (field_type == "struct") {
            // struct type
            check_layout_struct(jfield, field_name);
            auto struct_fields = columns_from_json(jfield, field_name);
            fields.push_back(
                arrow::field(field_name, arrow::struct_(struct_fields)));
        } else {
            // value type
            fields.push_back(
                arrow::field(field_name, datatype_from_string(field_type)));
        }
    }  // ifield
    return fields;
}

std::pair<std::vector<std::string>,
          std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>>
fill_field_builder_map_from_columns(
    const std::vector<std::shared_ptr<arrow::Field>>& columns) {
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
            throw parquetwriter::layout_exception(
                "Invalid data type for column \"" + column_name + "\"");
        }

        // get the names of any sub-struct typed fields of structures
        if (column_fill_type == parquetwriter::FillType::STRUCT) {
            auto [names, field_buffer_type_builders] =
                struct_type_field_builders(column_builder, column_name);
            for (size_t i = 0; i < field_buffer_type_builders.size(); i++) {
                std::stringstream sub_name;
                sub_name << column_name << "." << names.at(i);
                column_builders[sub_name.str()] =
                    field_buffer_type_builders.at(i);
                field_names.push_back(sub_name.str());
            }
        }

        out[column_name] = column_builders;
    }  // column iterator
    return std::make_pair(field_names, out);
}

std::pair<std::vector<std::string>, std::vector<arrow::ArrayBuilder*>>
struct_type_field_builders(arrow::ArrayBuilder* builder,
                           const std::string& column_name) {
    if (builder->type()->id() != arrow::Type::STRUCT) {
        throw parquetwriter::layout_exception(
            "Invalid ArrayBuilder type for column/field \"" + column_name +
            "\", expected type: \"struct\", received type: \"" +
            builder->type()->name() + "\"");
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
            throw parquetwriter::layout_exception(
                "Invalid list depth (depth>3) encountered in column/field \"" +
                column_name + "\"");
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
                throw parquetwriter::layout_exception(
                    "Column/field \"" + column_name + "\" is " +
                    std::to_string(depth) +
                    "D list of struct, but the terminal value builder for the "
                    "column/field is of type \"" +
                    terminal_builder->type()->name() + "\"");
            }
            if (!valid_sub_struct_layout(struct_builder, column_name)) {
                throw parquetwriter::layout_exception(
                    "Invalid struct-type nesting encountered in column/field "
                    "\"" +
                    column_name + "\"");
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
                    throw parquetwriter::layout_exception(
                        "Invalid struct-type nesting encountered in "
                        "column/field \"" +
                        column_name + "\"");
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
                    throw parquetwriter::layout_exception(
                        "Invalid list depth (depth>3) encountered in "
                        "column/field \"" +
                        column_name + "\"");
                } else if (sub_list_terminal_builder->type()->id() ==
                           arrow::Type::STRUCT) {
                    // check that sub-structs are "flat"
                    auto sub_struct_builder =
                        dynamic_cast<arrow::StructBuilder*>(
                            sub_list_terminal_builder);
                    if (!valid_sub_struct_layout(sub_struct_builder,
                                                 column_name)) {
                        throw parquetwriter::layout_exception(
                            "Invalid struct-type nesting encountered in "
                            "column/field \"" +
                            column_name + "\"");
                    }
                }
            }
        }
        return parquetwriter::FillType::STRUCT;
    }  // is_struct
    return parquetwriter::FillType::VALUE;
}

std::vector<std::string> struct_field_order_from_builder(
    arrow::ArrayBuilder* builder, const std::string& field_name) {
    static std::map<std::string, std::vector<std::string>> field_order_cache;
    if (field_order_cache.count(field_name) > 0) {
        return field_order_cache.at(field_name);
    }

    // either struct or struct-list builder can be provided
    arrow::StructBuilder* struct_builder = nullptr;
    if (builder->type()->id() == arrow::Type::LIST) {
        auto [depth, terminal_builder] = list_builder_description(
            dynamic_cast<arrow::ListBuilder*>(builder));
        if (terminal_builder->type()->id() != arrow::Type::STRUCT) {
            throw parquetwriter::writer_exception(
                "Expect value builder of type \"struct\" for column/field \"" +
                field_name + "\", but found  type \"" +
                terminal_builder->type()->name() + "\"");
        }
        struct_builder = dynamic_cast<arrow::StructBuilder*>(terminal_builder);
    } else if (builder->type()->id() == arrow::Type::STRUCT) {
        struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
    } else {
        throw parquetwriter::writer_exception(
            "Provided builder for column/field \"" + field_name +
            "\" is not of \"struct\" type");
    }

    auto field_buffer_type = struct_builder->type();

    unsigned n_fields = struct_builder->num_children();
    std::vector<std::string> non_struct_fields;
    for (size_t ifield = 0; ifield < n_fields; ifield++) {
        auto child_field_builder = struct_builder->child_builder(ifield).get();
        auto child_field_name = field_buffer_type->field(ifield)->name();
        if (builder_is_struct_type(child_field_builder)) continue;
        non_struct_fields.push_back(child_field_name);
    }  // ifield

    // cache it for later lookup
    field_order_cache[field_name] = non_struct_fields;
    return non_struct_fields;
}

bool builder_is_struct_type(arrow::ArrayBuilder* builder) {
    if (builder->type()->id() == arrow::Type::STRUCT) {
        // check struct type
        return true;
    } else if (builder->type()->id() == arrow::Type::LIST) {
        // check struct_list type
        auto [depth, terminal_builder] = list_builder_description(
            dynamic_cast<arrow::ListBuilder*>(builder));
        if (terminal_builder->type()->id() == arrow::Type::STRUCT) {
            return true;
        }
    }
    return false;
}

bool valid_sub_struct_layout(arrow::StructBuilder* struct_builder,
                             const std::string& parent_column_name) {
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
        if (builder_is_struct_type(child_field_builder)) {
            return false;
        }
        // if (child_field_builder->type()->id() == arrow::Type::STRUCT) {
        //    // disallow a field if it itself is a struct
        //    return false;
        //}  // is_struct
        // else if (child_field_builder->type()->id() == arrow::Type::LIST) {
        //    // disallow a field if it itself is a struct list
        //    size_t unpack_count = 0;
        //    auto list_builder =
        //        dynamic_cast<arrow::ListBuilder*>(child_field_builder);
        //    auto value_builder = list_builder->value_builder();
        //    while (value_builder->type()->id() == arrow::Type::LIST) {
        //        if (unpack_count >= 3) break;
        //        list_builder =
        //        dynamic_cast<arrow::ListBuilder*>(value_builder);
        //        value_builder = list_builder->value_builder();
        //        unpack_count++;
        //    }

        //    if (value_builder->type()->id() == arrow::Type::STRUCT) {
        //        return false;
        //    }
        //}  // is_list
    }  // ichild

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

std::string parent_column_name_from_field(const std::string& field_path) {
    size_t pos_parent = field_path.find_first_of(".");
    std::string parent_column_name = field_path;
    if (pos_parent != std::string::npos) {
        parent_column_name = field_path.substr(0, pos_parent);
    }
    return parent_column_name;
}

};  // namespace helpers
};  // namespace parquetwriter
