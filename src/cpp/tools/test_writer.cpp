#include "parquet_writer.h"
#include "parquet_writer_types.h"

//std/stl
#include <iostream>

//json
#include <nlohmann/json.hpp>

int main(int argc, char* argv[]) {

    namespace pw = parquetwriter;
    auto writer = std::make_unique<pw::Writer>();

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
                         ]}}
             ]})"_json;

    writer->load_schema(layout);
    writer->initialize_output(dataset_name, output_dir);
    writer->initialize_writer();

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
    pw::types::buffer_value_vec_t col13_data{col13_field_foo, col13_field_bar};
    // col14
    std::vector<pw::types::buffer_t> col14_data;
    for(size_t i = 0; i < 5; i++) {
        pw::types::buffer_value_vec_t col14_element_field_data;
        uint32_t col14_field_faz = 32;
        std::vector<int32_t> col14_field_baz{1,-2,3,-4};
        col14_element_field_data.push_back(col14_field_faz);
        col14_element_field_data.push_back(col14_field_baz);
        col14_data.push_back(col14_element_field_data);
    } // i


    // now fill the output table
    writer->fill("col0", {col0_data});
    writer->fill("col1", {col1_data});
    writer->fill("col2", {col2_data});
    writer->fill("col3", {col3_data});
    writer->fill("col4", {col4_data});
    writer->fill("col5", {col5_data});
    writer->fill("col6", {col6_data});
    writer->fill("col7", {col7_data});
    writer->fill("col8", {col8_data});
    writer->fill("col9", {col9_data});
    writer->fill("col10", {col10_data});
    writer->fill("col11", {col11_data});
    writer->fill("col12", {col12_data});
    writer->fill("col13", {col13_data});
    writer->fill("col14", {col14_data});

    writer->finish();

    return 0;
}
