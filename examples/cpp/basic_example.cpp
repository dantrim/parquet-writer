//
// examples of how to write all of the basic data types by
// creating a Parquet file with one column for each of the
// basic data types.
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
            {"name": "col0", "type": "bool"},
            {"name": "col1", "type": "int8"},
            {"name": "col2", "type": "int16"},
            {"name": "col3", "type": "int32"},
            {"name": "col4", "type": "int64"},
            {"name": "col5", "type": "uint8"},
            {"name": "col6", "type": "uint16"},
            {"name": "col7", "type": "uint32"},
            {"name": "col8", "type": "uint64"},
            {"name": "col9", "type": "float"},
            {"name": "col10", "type": "double"},
            {"name": "col11", "type": "list1d", "contains": {"type": "bool"}},
            {"name": "col12", "type": "list1d", "contains": {"type": "int8"}},
            {"name": "col13", "type": "list1d", "contains": {"type": "int16"}},
            {"name": "col14", "type": "list1d", "contains": {"type": "int32"}},
            {"name": "col15", "type": "list1d", "contains": {"type": "int64"}},
            {"name": "col16", "type": "list1d", "contains": {"type": "uint8"}},
            {"name": "col17", "type": "list1d", "contains": {"type": "uint16"}},
            {"name": "col18", "type": "list1d", "contains": {"type": "uint32"}},
            {"name": "col19", "type": "list1d", "contains": {"type": "uint64"}},
            {"name": "col20", "type": "list1d", "contains": {"type": "float"}},
            {"name": "col21", "type": "list1d", "contains": {"type": "double"}},
            {"name": "col22", "type": "list2d", "contains": {"type": "bool"}},
            {"name": "col23", "type": "list2d", "contains": {"type": "int8"}},
            {"name": "col24", "type": "list2d", "contains": {"type": "int16"}},
            {"name": "col25", "type": "list2d", "contains": {"type": "int32"}},
            {"name": "col26", "type": "list2d", "contains": {"type": "int64"}},
            {"name": "col27", "type": "list2d", "contains": {"type": "uint8"}},
            {"name": "col28", "type": "list2d", "contains": {"type": "uint16"}},
            {"name": "col29", "type": "list2d", "contains": {"type": "uint32"}},
            {"name": "col30", "type": "list2d", "contains": {"type": "uint64"}},
            {"name": "col31", "type": "list2d", "contains": {"type": "float"}},
            {"name": "col32", "type": "list2d", "contains": {"type": "double"}},
            {"name": "col33", "type": "list3d", "contains": {"type": "bool"}},
            {"name": "col34", "type": "list3d", "contains": {"type": "int8"}},
            {"name": "col35", "type": "list3d", "contains": {"type": "int16"}},
            {"name": "col36", "type": "list3d", "contains": {"type": "int32"}},
            {"name": "col37", "type": "list3d", "contains": {"type": "int64"}},
            {"name": "col38", "type": "list3d", "contains": {"type": "uint8"}},
            {"name": "col39", "type": "list3d", "contains": {"type": "uint16"}},
            {"name": "col40", "type": "list3d", "contains": {"type": "uint32"}},
            {"name": "col41", "type": "list3d", "contains": {"type": "uint64"}},
            {"name": "col42", "type": "list3d", "contains": {"type": "float"}},
            {"name": "col43", "type": "list3d", "contains": {"type": "double"}}
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
            "dataset_name": "example",
            "foo": "bar",
            "n_things": 42,
            "things": {"foo": "bar"}
        }
    }
    )"_json;

    // create and initialize the parquetwriter::Writer
    namespace pw = parquetwriter;
    pw::logging::set_debug();  // set debug for the examples

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
    // create dummy data for each of the columns
    //

    // columns containing basic data types
    bool col0_data{true};
    int8_t col1_data{127};
    int16_t col2_data{32767};
    int32_t col3_data{424242424};
    int64_t col4_data{42424242424};
    uint8_t col5_data{242};
    uint16_t col6_data{42424};
    uint32_t col7_data{4242424242};
    uint64_t col8_data{424242424242};
    float col9_data{42.5};
    double col10_data{424242.5};

    // columns containing one-dimensional lists of basic data types
    std::vector<bool> col11_data{true, false, true, false, true};
    std::vector<int8_t> col12_data{col1_data, col1_data, col1_data};
    std::vector<int16_t> col13_data{col2_data, col2_data, col2_data, col2_data};
    std::vector<int32_t> col14_data{col3_data, col3_data, col3_data};
    std::vector<int64_t> col15_data{col4_data, col4_data, col4_data, col4_data};
    std::vector<uint8_t> col16_data{col5_data, col5_data, col5_data};
    std::vector<uint16_t> col17_data{col6_data, col6_data, col6_data,
                                     col6_data};
    std::vector<uint32_t> col18_data{col7_data, col7_data, col7_data};
    std::vector<uint64_t> col19_data{col8_data, col8_data, col8_data,
                                     col8_data};
    std::vector<float> col20_data{col9_data, col9_data, col9_data, col9_data};
    std::vector<double> col21_data{col10_data, col10_data, col10_data};

    // columns containing two-dimensional lists of basic data types
    std::vector<std::vector<bool>> col22_data{col11_data, col11_data,
                                              col11_data};
    std::vector<std::vector<int8_t>> col23_data{col12_data, col12_data,
                                                col12_data, col12_data};
    std::vector<std::vector<int16_t>> col24_data{
        col13_data, col13_data, col13_data, col13_data, col13_data};
    std::vector<std::vector<int32_t>> col25_data{col14_data, col14_data,
                                                 col14_data, col14_data};
    std::vector<std::vector<int64_t>> col26_data{col15_data, col15_data,
                                                 col15_data};
    std::vector<std::vector<uint8_t>> col27_data{col16_data, col16_data,
                                                 col16_data, col16_data};
    std::vector<std::vector<uint16_t>> col28_data{
        col17_data, col17_data, col17_data, col17_data, col17_data};
    std::vector<std::vector<uint32_t>> col29_data{col18_data, col18_data,
                                                  col18_data, col18_data};
    std::vector<std::vector<uint64_t>> col30_data{col19_data, col19_data,
                                                  col19_data};
    std::vector<std::vector<float>> col31_data{col20_data, col20_data,
                                               col20_data, col20_data};
    std::vector<std::vector<double>> col32_data{col21_data, col21_data,
                                                col21_data};

    // columns containing three-dimensional lists of basic data types
    std::vector<std::vector<std::vector<bool>>> col33_data{col22_data,
                                                           col22_data};
    std::vector<std::vector<std::vector<int8_t>>> col34_data{
        col23_data, col23_data, col23_data};
    std::vector<std::vector<std::vector<int16_t>>> col35_data{
        col24_data, col24_data, col24_data, col24_data};
    std::vector<std::vector<std::vector<int32_t>>> col36_data{
        col25_data, col25_data, col25_data};
    std::vector<std::vector<std::vector<int64_t>>> col37_data{col26_data,
                                                              col26_data};
    std::vector<std::vector<std::vector<uint8_t>>> col38_data{
        col27_data, col27_data, col27_data};
    std::vector<std::vector<std::vector<uint16_t>>> col39_data{
        col28_data, col28_data, col28_data, col28_data};
    std::vector<std::vector<std::vector<uint32_t>>> col40_data{
        col29_data, col29_data, col29_data};
    std::vector<std::vector<std::vector<uint64_t>>> col41_data{col30_data,
                                                               col30_data};
    std::vector<std::vector<std::vector<float>>> col42_data{
        col31_data, col31_data, col31_data};
    std::vector<std::vector<std::vector<double>>> col43_data{col32_data,
                                                             col32_data};

    //
    // fill a couple of rows with the same set of dummy data in each
    //
    for (size_t irow = 0; irow < 10; irow++) {
        // basic data types
        writer.fill("col0", col0_data);
        writer.fill("col1", col1_data);
        writer.fill("col2", col2_data);
        writer.fill("col3", col3_data);
        writer.fill("col4", col4_data);
        writer.fill("col5", col5_data);
        writer.fill("col6", col6_data);
        writer.fill("col7", col7_data);
        writer.fill("col8", col8_data);
        writer.fill("col9", col9_data);
        writer.fill("col10", col10_data);

        // one-dimensional lists
        writer.fill("col11", col11_data);
        writer.fill("col12", col12_data);
        writer.fill("col13", col13_data);
        writer.fill("col14", col14_data);
        writer.fill("col15", col15_data);
        writer.fill("col16", col16_data);
        writer.fill("col17", col17_data);
        writer.fill("col18", col18_data);
        writer.fill("col19", col19_data);
        writer.fill("col20", col20_data);
        writer.fill("col21", col21_data);

        // two-dimensional lists
        writer.fill("col22", col22_data);
        writer.fill("col23", col23_data);
        writer.fill("col24", col24_data);
        writer.fill("col25", col25_data);
        writer.fill("col26", col26_data);
        writer.fill("col27", col27_data);
        writer.fill("col28", col28_data);
        writer.fill("col29", col29_data);
        writer.fill("col30", col30_data);
        writer.fill("col31", col31_data);
        writer.fill("col32", col32_data);

        // three-dimensional lists
        writer.fill("col33", col33_data);
        writer.fill("col34", col34_data);
        writer.fill("col35", col35_data);
        writer.fill("col36", col36_data);
        writer.fill("col37", col37_data);
        writer.fill("col38", col38_data);
        writer.fill("col39", col39_data);
        writer.fill("col40", col40_data);
        writer.fill("col41", col41_data);
        writer.fill("col42", col42_data);
        writer.fill("col43", col43_data);

        // finish handling the current row
        writer.end_row();
    }  // irow

    //
    // call finish to close the output
    //
    writer.finish();

    return 0;
}
