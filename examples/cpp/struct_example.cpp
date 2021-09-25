//
// example showing how to write structs to an output Parquet
// file, in addition to more complex structures composed
// of structs.
//

// parquet-writer
#include "parquet_writer.h"

// std/stl
#include "stdint.h"

// json
#include "nlohmann/json.hpp"

int main(int argc, char* argv[]) {
    //
    // specify the Parquet file data layout (names are arbitrary),
    // can be taken from a file but here we use a JSON string literal
    //
    // note: anywhere where a JSON object is used, can also be replaced
    //          with a JSON object serialized to std::string
    //
    auto layout = R"(
    {
        "fields": [
            {"name": "basic_struct", "type": "struct",
                        "fields":[
                                  {"name": "float_field", "type": "float"},
                                  {"name": "int_field", "type": "int32"},
                                  {"name": "list_field", "type": "list1d",
                                                                 "contains": {"type": "int32"}}
                                 ]},
            {"name": "struct_list1d", "type": "list1d",
                        "contains": {"type": "struct", "fields":[
                                               {"name": "float_field", "type": "float"},
                                               {"name": "int_field", "type": "int32"},
                                               {"name": "list_field", "type": "list1d",
                                                    "contains": {"type": "int32"}}
                                             ]
                                    }},
            {"name": "struct_with_struct", "type": "struct",
                        "fields":[
                            {"name": "float_field", "type": "float"},
                            {"name": "int_field", "type": "int32"},
                            {"name": "list_field", "type": "list1d",
                                            "contains": {"type": "int32"}},
                            {"name": "struct_field", "type": "struct",
                                "fields":[
                                    {"name": "float_field", "type": "float"},
                                    {"name": "int_field", "type": "int32"},
                                    {"name": "list_field", "type": "list1d",
                                                "contains": {"type": "int32"}}
                            ]}
                
                        ]},
            {"name": "struct_with_struct_list", "type": "struct",
                        "fields":[
                            {"name": "float_field", "type": "float"},
                            {"name": "int_field", "type": "int32"},
                            {"name": "list_field", "type": "list1d",
                                            "contains": {"type": "int32"}},
                            {"name": "struct_list", "type": "list1d",
                                        "contains": {"type": "struct", "fields":[
                                            {"name": "float_field", "type": "float"},
                                            {"name": "int_field", "type": "int32"},
                                            {"name": "list_field", "type": "list1d",
                                                        "contains": {"type": "int32"}}
                                        ]}}
                        ]},
            {"name": "struct_with_struct_list2d", "type": "struct",
                "fields":[
                    {"name": "float_field", "type": "float"},
                    {"name": "struct_list", "type": "list2d",
                        "contains": {"type": "struct", "fields":[
                                        {"name": "float_field", "type": "float"},
                                        {"name": "int_field", "type": "int32"},
                                        {"name": "list_field", "type": "list1d",
                                                "contains": {"type": "int32"}}
                                    ]}}]
                }
        ]
    }
    )"_json;

    //
    // provide arbitrary keyvalue metadata JSON object to store in the Parquet
    // file
    //
    // note: anywhere where a JSON object is used, can also be replaced
    //          with a JSON object serialized to std::string
    //
    auto metadata = R"(
    {
        "metadata": {
            "dataset_name": "struct_example",
            "foo": "bar",
            "n_things": 42,
            "things": {"foo": "bar"}
        }
    }
    )"_json;

    // create and initialize the parquetwriter::Writer
    namespace pw = parquetwriter;
    pw::logging::set_debug();  // set debug for examples

    pw::Writer writer;
    writer.set_layout(layout);
    writer.set_dataset_name("example_dataset");
    writer.set_output_directory("example_dataset");
    writer.set_metadata(metadata);  // optional
    writer.set_compression(
        pw::Compression::UNCOMPRESSED);  // or SNAPPY or GZIP (default is
                                         // UNCOMPRESSED)
    writer.initialize();

    //
    // create dummy data for each of the fields of the struct
    //
    float float_field_data = 42.5;
    int32_t int_field_data = 42;
    std::vector<int32_t> list_field_data{1, 2, 3, 4, -5, -6, -7, -8, -9, -10};

    // the "basic_struct" column holds a single struct element in each row (it
    // is flat)
    pw::field_map_t basic_struct_data{ {"float_field", float_field_data},
                                       {"int_field", int_field_data},
                                       {"list_field", list_field_data} };
    //pw::struct_t basic_struct_data{float_field_data, int_field_data,
    //                               list_field_data};

    // the "struct_list1d" column holds a list of struct elements in each row
    // (here the list length is arbitrarily set to 7)
    pw::struct_list1d struct_list_data;
    for (size_t i = 0; i < 7; i++) {
        pw::field_map_t struct_map{ {"float_field", float_field_data},
                                    {"int_field", int_field_data},
                                    {"list_field", list_field_data} };
        pw::struct_t struct_data = writer.to_struct("struct_list1d", struct_map);
        struct_list_data.push_back(struct_data);
    }  // i

    pw::struct_t float_field{float_field_data};
    pw::struct_list2d struct_list2d_data;
    for (size_t i = 0; i < 5; i++) {
        std::vector<pw::struct_t> inner_data;
        for (size_t j = 0; j < 2; j++) {
            pw::struct_t data{float_field_data, int_field_data,
                              list_field_data};
            inner_data.push_back(data);
        }
        struct_list2d_data.push_back(inner_data);
    }

    //
    // fill a couple of rows with the same set of dummy data in each
    //
    for (size_t irow = 0; irow < 10; irow++) {
        // basic_struct
        writer.fill("basic_struct", {basic_struct_data});

        // one-dimensional list of structs
        writer.fill("struct_list1d", {struct_list_data});

        writer.fill("struct_with_struct_list2d", {float_field});
        writer.fill("struct_with_struct_list2d.struct_list",
                    {struct_list2d_data});

        // struct with struct field
        writer.fill("struct_with_struct", {basic_struct_data});
        writer.fill("struct_with_struct.struct_field", {basic_struct_data});

        // struct with a field that is a list of structs
        // note #1: the outer struct must always have fields to be filled if the
        // inner struct is to be filled note #2: fill the internal structs'
        // fields independently of the outer structs' fields
        writer.fill("struct_with_struct_list", {basic_struct_data});
        writer.fill("struct_with_struct_list.struct_list", {struct_list_data});

        // finish handling the current row
        writer.end_row();
    }  // irow

    //
    // call finish to close the output
    //
    writer.finish();

    return 0;
}
