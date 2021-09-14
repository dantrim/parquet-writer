#include "parquet_writer.h"
#include "parquet_writer_types.h"

//std/stl
#include <iostream>

//json
#include <nlohmann/json.hpp>

int main(int argc, char* argv[]) {

    namespace pw = parquetwriter;
    pw::Writer writer;

    std::string dataset_name = "dummy_dataset";
    std::string output_dir = "dummy_dataset";


    //
    // Parquet file layout specified via JSON
    //
    auto layout = R"(
        {
            "fields": [
                {"name": "col0", "type": "int8"},
                {"name": "col1", "type": "int16"},
                {"name": "col2", "type": "int32"},
                {"name": "col3", "type": "int64"},
                {"name": "col4", "type": "uint8"},
                {"name": "col5", "type": "uint16"},
                {"name": "col6", "type": "uint32"},
                {"name": "col7", "type": "uint64"},
                {"name": "col8", "type": "float32"},
                {"name": "col9", "type": "float64"},
                {"name": "col10", "type": "list", "contains" : {"type": "int32"}},
                {"name": "col11", "type": "list", "contains" : {"type": "float32"}},
                {"name": "col12", "type": "bool"},
                {"name": "col13", "type": "struct", "fields":
                         [ {"name": "foo", "type": "uint32"},
                           {"name": "bar", "type": "float64"}
                         ]},
                {"name": "col14", "type": "list", "contains" : {"type": "struct", "fields":
                         [ {"name": "faz", "type": "uint32"},
                           {"name": "baz", "type": "list", "contains" : {"type": "int32"}}
                         ]}},
                {"name": "col15", "type": "list", "contains" : {"type": "list", "contains": {"type": "int32"}}},
                {"name": "col16", "type": "list", "contains" : {"type": "list", "contains": {"type": "float32"}}},
                {"name": "col17", "type": "list",
                                            "contains" : {"type": "list",
                                            "contains": {"type": "list", "contains": {"type": "uint32"}}}},
                {"name": "col18", "type": "list",
                                            "contains": {"type": "list",
                                            "contains": {"type": "struct", "fields":
                                                [ {"name": "foo", "type": "float32"}, {"name": "bar", "type": "int32"} ]
                                            }}},
                {"name": "col19", "type": "list",
                                            "contains": {"type": "list",
                                            "contains": {"type": "list", 
                                            "contains": {"type": "struct", "fields":
                                                [ {"name": "foo", "type": "float32"}, {"name": "bar", "type": "int32"} ]}
                                            }}}
            
             ]})"_json;

    auto metadata = R"(
        {
            "dataset": "foobar",
            "creation_data": "2021/09/14"
        }
    )"_json;

    unsigned n_fields = 20;

    writer.set_layout(layout);
    writer.set_flush_rule(pw::FlushRule::NROWS, 250000 / n_fields);
    writer.set_metadata(metadata);
    writer.set_dataset_name(dataset_name);
    writer.set_output_directory(output_dir);
    writer.initialize();

    //
    // create some dummy data and fill
    //

    // col0
    int8_t col0_data = 7;
    // col1
    int16_t col1_data = -232;
    // col2
    int32_t col2_data = 1023;
    // col3
    int64_t col3_data = -10232;
    // col4
    uint8_t col4_data = 3;
    // col5
    uint16_t col5_data = 232;
    // col6
    uint32_t col6_data = 2046;
    // col7
    uint64_t col7_data = 4098;
    // col8
    float col8_data = 1023.8;
    // col9
    double col9_data = 50.2;
    // col10
    std::vector<int32_t> col10_data{1,-2, 3, -4};
    // col11
    std::vector<float> col11_data{1.5, -2.5, 3.5, -4.5};
    // col12
    bool col12_data = true;
    // col13
    uint32_t col13_field_foo = 42;
    double col13_field_bar = 103.7;
    pw::struct_element col13_data{col13_field_foo, col13_field_bar};
    // col14
    pw::struct_list1d col14_data;
    for(size_t i = 0; i < 5; i++) {
        pw::struct_element col14_element_field_data;
        uint32_t col14_field_faz = 32;
        std::vector<int32_t> col14_field_baz{1,-2,3,-4};
        col14_element_field_data.push_back(col14_field_faz);
        col14_element_field_data.push_back(col14_field_baz);
        col14_data.push_back(col14_element_field_data);
    } // i
    // col15
    std::vector<std::vector<int32_t>> col15_data{ {1,2,3}, {-4,-5,-6, -7, -8},  {} };
    // col16
    std::vector<std::vector<float>> col16_data{ {1.2, 3.4, 5.6}, {}, {-1.2, -3.4, -5.6, -7.8}, {-3.9, 42.5} };
    // col17
    std::vector<std::vector<std::vector<uint32_t>>> col17_data{
        { {0,1,2}, {3,4,5,6,7}, {42} },
        { {19}, {12, 13, 14}, {52, 57, 99, 0}, {22} }
    };

    // col18
    pw::struct_list2d col18_data;
    for(size_t i = 0; i < 3; i++) {
        std::vector<pw::struct_element> inner_data;
        for(size_t j = 0; j < (i+1); j++) {
            pw::struct_element col18_element_field_data;
            float col18_field_foo = i * j;
            int32_t col18_field_bar = i*j + 2;
            col18_element_field_data.push_back(col18_field_foo);
            col18_element_field_data.push_back(col18_field_bar);
            inner_data.push_back(col18_element_field_data);
        } // j
        col18_data.push_back(inner_data);
    } // i

    // col19
    pw::struct_list3d col19_data;
    for(size_t i = 0; i < 5; i++) {
        std::vector<std::vector<pw::struct_element>> inner3_data;
        for(size_t j = 0; j < 3; j++) {
            std::vector<pw::struct_element> inner2_data;
            for(size_t k = 0; k < 2; k++) {
                pw::struct_element col19_element_field_data;
                float col19_field_foo = i * j * k;
                int32_t col19_field_bar = i*j * k + 2;
                col19_element_field_data.push_back(col19_field_foo);
                col19_element_field_data.push_back(col19_field_bar);
                inner2_data.push_back(col19_element_field_data);
            } // k
            inner3_data.push_back(inner2_data);
        } // j
        col19_data.push_back(inner3_data);
    } // i


    // now fill the output table
    for(size_t ievent = 0; ievent < 10; ievent++) {
        writer.fill("col0", {col0_data});
        writer.fill("col1", {col1_data});
        writer.fill("col2", {col2_data});
        writer.fill("col3", {col3_data});
        writer.fill("col4", {col4_data});
        writer.fill("col5", {col5_data});
        writer.fill("col6", {col6_data});
        writer.fill("col7", {col7_data});
        writer.fill("col8", {col8_data});
        writer.fill("col9", {col9_data});
        writer.fill("col10", {col10_data});
        writer.fill("col11", {col11_data});
        writer.fill("col12", {col12_data});
        writer.fill("col13", {col13_data});
        writer.fill("col14", {col14_data});
        writer.fill("col15", {col15_data});
        writer.fill("col16", {col16_data});
        writer.fill("col17", {col17_data});
        writer.fill("col18", {col18_data});
        writer.fill("col19", {col19_data});
    }

    writer.finish();

    return 0;
}
