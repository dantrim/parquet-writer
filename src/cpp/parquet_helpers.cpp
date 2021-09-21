#include "parquet_helpers.h"

#include "logging.h"

// std/stl
#include <sstream>
namespace parquetwriter {
namespace helpers {

ColumnWrapper::ColumnWrapper(std::string name)
    : _name(name), _builder(nullptr) {}

void ColumnWrapper::create_builder(std::shared_ptr<arrow::DataType> type) {
    auto pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> tmp;
    WRITER_CHECK_RESULT(arrow::MakeBuilder(pool, type, &tmp));
    _builder = tmp.release();
}

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
                                fields_from_json(jcontains3, field_name);
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
                            fields_from_json(jcontains2, field_name);
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
                        fields_from_json(jcontains, field_name);
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
                auto struct_fields = fields_from_json(jfield, field_name);
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

std::map<std::string, arrow::ArrayBuilder*> makeVariableMap(
    std::shared_ptr<ColumnWrapper> node) {
    auto builder = node->builder();
    std::map<std::string, arrow::ArrayBuilder*> out_map;
    makeVariableMap(builder, node->name(), "", out_map);
    return out_map;
}

void makeVariableMap(arrow::ArrayBuilder* builder, std::string parentname,
                     std::string prefix,
                     std::map<std::string, arrow::ArrayBuilder*>& out_map) {
    auto type = builder->type();
    //if (builder->num_children() > 0) {
    if (type->id() == arrow::Type::STRUCT) {
        std::string struct_builder_name = parentname;  // + "/";
        out_map[struct_builder_name] = builder;
        for (size_t ichild = 0; ichild < builder->num_children(); ichild++) {
            auto field = type->field(ichild);
            auto child_builder = builder->child_builder(ichild).get();
            auto child_type = child_builder->type();
            auto n_child_children = child_builder->num_children();
            bool child_is_nested = (child_builder->num_children() > 0);
            bool child_is_list = (child_type->id() == arrow::Type::LIST);
            if (child_is_nested) {
                std::string this_name =
                    parentname + "." + field->name();  // + "/";
                out_map[this_name] = child_builder;

                std::string child_name = parentname + "." + field->name();
                makeVariableMap(child_builder, child_name, field->name(),
                                out_map);
            } else if (child_is_list) {
                arrow::ListBuilder* list_builder =
                    static_cast<arrow::ListBuilder*>(child_builder);
                auto item_builder = list_builder->value_builder();
                std::string outname = parentname + "." + field->name();
                std::string list_name = outname;  // + "/list";
                std::string val_name = outname + "/item";
                out_map[list_name] = child_builder;
                out_map[val_name] =
                    item_builder;  // dynamic_cast<arrow::ArrayBuilder*>(item_builder);

                //if(item_builder->type()->id() == arrow::Type::LIST) {
                //    auto item2_builder = dynamic_cast<arrow::ListBuilder*>(item_builder)->value_builder();
                //    std::string val2_name = val_name + "/item";
                //    out_map[val2_name] = item2_builder;

                //    if(item2_builder->type()->id() == arrow::Type::LIST) {
                //        auto item3_builder = dynamic_cast<arrow::ListBuilder*>(item_builder)->value_builder();
                //        std::string val3_name = val_name + "/item";
                //        out_map[val3_name] = item3_builder;
                //    }
                //}
            } else {
                std::string outname = parentname + "." + field->name();
                out_map[outname] = child_builder;
            }
        }  // ichild
    } else if (type->id() == arrow::Type::LIST) {
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        std::string outname = parentname;
        if (prefix != "") {
            outname = prefix + "." + outname;
        }
        std::string list_name = outname;  // + "/list";
        out_map[list_name] = list_builder;
        std::string val_name = outname + "/item";
        auto item_builder = list_builder->value_builder();
        out_map[val_name] = item_builder;

        //if(item_builder->type()->id() == arrow::Type::LIST) {
        //    auto item2_builder = dynamic_cast<arrow::ListBuilder*>(item_builder)->value_builder();
        //    std::string val2_name = val_name + "/item";
        //    out_map[val2_name] = item2_builder;

        //    if(item2_builder->type()->id() == arrow::Type::LIST) {
        //        auto item3_builder = dynamic_cast<arrow::ListBuilder*>(item_builder)->value_builder();
        //        std::string val3_name = val2_name + "/item";
        //        out_map[val3_name] = item3_builder;
        //    }
        //}
    } else {
        std::string outname = parentname;
        if (prefix != "") {
            outname = prefix + "." + outname;
        }
        out_map[outname] = builder;
    }
}

std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
col_builder_map_from_fields(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> out;
    for (auto field : fields) {
        auto field_name = field->name();
        auto field_type = field->type();
        auto node = std::make_shared<ColumnWrapper>(field_name);
        node->create_builder(field_type);
        out[field_name] = makeVariableMap(node);

        auto log = logging::get_logger();
        log->debug("{0} - ============= {1} MAP ===========",
                   __PRETTYFUNCTION__, field_name);
        for (const auto& [key, val] : makeVariableMap(node)) {
            log->debug("{0} - key = {1}, val type = {2}", __PRETTYFUNCTION__,
                       key, val->type()->name());
        }
    }
    return out;
}

std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
fill_field_builder_map_from_columns(const std::vector<std::shared_ptr<arrow::Field>>& columns) {

    //
    // we want to create an entry for each column and then
    // a sub-map for each builder associated with a call
    // to parquetwriter::Writer::fill
    //

    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> out;

    for( auto column : columns ) {
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


        // keep track of the fill type for this column
        //parquetwriter::FillType fill_type = parquetwriter::FillType::VALUE;

        parquetwriter::FillType column_fill_type = fill_type_from_column(column_builder);

        if(column_fill_type == parquetwriter::FillType::INVALID) {
            throw std::runtime_error("Bad column \"" + column_name + "\"");
        }
    } // column iterator
}

parquetwriter::FillType fill_type_from_column(arrow::ArrayBuilder* column_builder) {

    parquetwriter::FillType fill_type = parquetwriter::FillType::INVALID;

    std::string column_name = column_builder->name();

    //
    // if there are any nested data structures, get the associated builders
    // 
    bool is_list1d = column_builder->type()->id() == arrow::Type::LIST;
    bool is_struct = column_builder->type()->id() == arrow::Type::STRUCT;

    // For FillTypes::VALUE_LIST_{1D,2D,3D} we do not need entries
    // for the value_builders since they can always be inferred from the
    // top level

    if (is_list1d) {
        auto value_builder = dynamic_cast<ListBuilder*>(column_builder);
        bool is_list2d = value_builder->type()->id() == arrow::Type::LIST;
        if(is_list2d) {
            value_builder = dynamic_cast<ListBuilder*>(value_builder)->value_builder();
            bool is_list3d = value_builder->type()->id() == arrow::Type::LIST;
            if(is_list3d) {
                //
                // 3D list
                //

                // we do not support more than 3D list, so if this builder
                // is of type list then throw an exception
                value_builder = dynamic_cast<ListBuilder*>(value_builder)->value_builder();
                bool is_list4d = value_builder->type()->id() == arrow::Type::LIST;
                if(is_list4d) {
                    std::stringstream err;
                    err << "List depth >3 not supported, bad column \"" << column_name << "\"";
                    log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                    throw std::runtime_error(err.str());
                } // is_list4d

                // check if struct_list
                bool is_struct_list3d = value_builder->type()->id() == arrow::Type::STRUCT;
                if(is_struct_list3d) {
                    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(value_builder);
                    bool valid_struct = validate_sub_struct_layout(struct_builder, column_name);
                    if(!valid_struct) {
                        throw std::runtime_error("Invalid layout for column \"" + column_name + "\"");
                    }
                    fill_type = parquetwriter::FillType::STRUCT_LIST_3D;
                } else {
                    fill_type = parquetwriter::FillType::VALUE_LIST_3D;
                }
            } // is_list3d
            else {
                //
                // 2D list
                //

                // check if struct_list
                bool is_struct_list2d = value_builder->type()->id() == arrow::Type::STRUCT;
                if(is_struct_list2d) {
                    auto struct_builder = dynamic_cast<arrow::StructBuilder*>(value_builder);
                    bool valid_struct = validate_sub_struct_layout(struct_builder, column_name);
                    if(!valid_struct) {
                        throw std::runtime_error("Invalid layout for column \"" + column_name + "\"");
                    }
                    fill_type = parquetwriter::FillType::STRUCT_LIST_2D;
                } else {
                    fill_type = parquetwriter::FillType::VALUE_LIST_2D;
                }
            }

        } // is_list2d
        else {

            //
            // 1D list
            //

            // check if struct_list
            bool is_struct_list1d = value_builder->type()->id() == arrow::Type::STRUCT;
            if(is_struct_list1d) {
                auto struct_builder = dynamic_cast<arrow::StructBuilder*>(value_builder);
                bool valid_struct = validate_sub_struct_layout(struct_builder, column_name);
                if(!valid_struct) {
                    throw std::runtime_error("Invalid layout for column \"" + column_name + "\"");
                }
                fill_type = parquetwriter::FillType::STRUCT_LIST_1D;
            } else {
                // list of values
                fill_type = parquetwriter::FillType::VALUE_LIST_1D;
            }
        } // !is_list2d
    } // is_list1d
    else if(is_struct) {

        auto struct_builder = dynamic_cast<arrow::StructBuilder*>(column_builder);
        unsigned number_of_fields = struct_builder->num_children();
        for(size_t ichild = 0; ichild < number_of_fields; ichild++) {
            auto child = struct_builder->child_builder(ichild).get();
            if(child->type()->id() == arrow::Type::STRUCT) {
            }
        } // ichild
        fill_type = parquetwriter::FillType::STRUCT;
    } else {

        fill_type = parquetwriter::FillType::VALUE;
    }

    return fill_type;
}

bool validate_sub_struct_layout(arrow::StructBuilder* struct_builder
        const std::string& parent_column_name) {

    //
    // any struct type that is itself a child field of some nested type
    // cannot itself have any terminal fields that are structs: i.e.
    // struct lists and structs are not allowd
    //
    unsigned total_number_of_fields = struct_builder->num_children();
    for(size_t ichild = 0; ichild < total_number_of_fields; ichild++) {
        auto child_field_builder = struct_builder->child_builder(ichild).get();
        auto child_field_name = child_field_builder->name();

        //
        // no sub struct allowed
        //
        if(child_field_builder->type()->id() == arrow::Type::STRUCT) {
            //
            // disallow a field if it itself is a struct
            //
            std::stringstream err;
            err << "Child struct of column \"" << parent_column_name << "\" has invalid field \"" << child_field_name << "\" with type \"struct\"";
            log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
            return false;
        } // is_struct
        else if(child_field_builder->type()->id() == arrow::Type::LIST) {

            //
            // disallow a field if it itself is a struct list
            //

            size_t unpack_count = 0;
            auto list_builder = dynamic_cast<arrow::ListBuilder*>(child_field_builder);
            auto value_builder = list_builder->value_builder();
            while (value_builder->type()->id() == arrow::Type::LIST) {
                if(unpack_count >= 3) break;
                list_builder = dynamic_cast<arrow::ListBuilder*>(value_builder);
                value_builder = list_builder->value_builder();
                unpack_count++;
            }

            if(value_builder->type()->id() == arrow::Type::STRUCT) {
                std::stringstream err;
                err << "Child struct of column \"" << parent_column_name << "\"  has invalid field \"" << child_field_name << "\" with type \"struct list\"";
                log->error("{0} - {1}", __PRETTYFUNCTION__, err.str());
                return false;
            }
        } // is_list
    } // ichild

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
