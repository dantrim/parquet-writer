#pragma once

#include "parquet_writer_fill_types.h"
#include "parquet_writer_types.h"

// std/stl
#include <map>
#include <memory>
#include <string>
#include <vector>

// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/type.h>  // struct_
#include <parquet/exception.h>

// json
#include "nlohmann/json.hpp"

namespace parquetwriter {
namespace helpers {

namespace internal {
typedef std::shared_ptr<arrow::DataType> type_ptr;

struct ArrowTypeInit {
    type_ptr get_bool() { return arrow::boolean(); }
    type_ptr get_int8() { return arrow::int8(); }
    type_ptr get_int16() { return arrow::int16(); }
    type_ptr get_int32() { return arrow::int32(); }
    type_ptr get_int64() { return arrow::int64(); }
    type_ptr get_uint8() { return arrow::uint8(); }
    type_ptr get_uint16() { return arrow::uint16(); }
    type_ptr get_uint32() { return arrow::uint32(); }
    type_ptr get_uint64() { return arrow::uint64(); }
    type_ptr get_float32() { return arrow::float32(); }
    type_ptr get_float64() { return arrow::float64(); }
    type_ptr get_string() { return arrow::utf8(); }

};  // ArrowTypeInit

typedef type_ptr (ArrowTypeInit::*TypeInitFunc)(void);

static std::map<std::string, TypeInitFunc> const type_init_map = {
    {"bool", &parquetwriter::helpers::internal::ArrowTypeInit::get_bool},
    {"int8", &parquetwriter::helpers::internal::ArrowTypeInit::get_int8},
    {"int16", &parquetwriter::helpers::internal::ArrowTypeInit::get_int16},
    {"int32", &parquetwriter::helpers::internal::ArrowTypeInit::get_int32},
    {"int64", &parquetwriter::helpers::internal::ArrowTypeInit::get_int64},
    {"uint8", &parquetwriter::helpers::internal::ArrowTypeInit::get_uint8},
    {"uint16", &parquetwriter::helpers::internal::ArrowTypeInit::get_uint16},
    {"uint32", &parquetwriter::helpers::internal::ArrowTypeInit::get_uint32},
    {"uint64", &parquetwriter::helpers::internal::ArrowTypeInit::get_uint64},
    {"float", &parquetwriter::helpers::internal::ArrowTypeInit::get_float32},
    {"double", &parquetwriter::helpers::internal::ArrowTypeInit::get_float64},
    {"string", &parquetwriter::helpers::internal::ArrowTypeInit::get_string}};

};  // namespace internal

using nlohmann::json;

std::shared_ptr<arrow::DataType> datatype_from_string(
    const std::string& type_string);
std::vector<std::shared_ptr<arrow::Field>> columns_from_json(
    const json& jlayout, const std::string& current_node = "");

void check_layout_list(const nlohmann::json& layout,
                       const std::string& column_name);
void check_layout_struct(const nlohmann::json& layout,
                         const std::string& column_name);

std::pair<std::vector<std::string>,
          std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>>
fill_field_builder_map_from_columns(
    const std::vector<std::shared_ptr<arrow::Field>>& columns);

parquetwriter::FillType column_filltype_from_builder(
    arrow::ArrayBuilder* column_builder, const std::string& column_name);

bool valid_sub_struct_layout(arrow::StructBuilder* struct_builder,
                             const std::string& parent_column_name);
std::pair<unsigned, arrow::ArrayBuilder*> list_builder_description(
    arrow::ListBuilder* list_builder);
std::pair<std::vector<std::string>, std::vector<arrow::ArrayBuilder*>>
struct_type_field_builders(arrow::ArrayBuilder* builder,
                           const std::string& column_name);

parquetwriter::struct_t struct_from_data_buffer_element(
    const parquetwriter::types::buffer_t& data, const std::string& field_name);

std::pair<unsigned, unsigned> field_nums_from_struct(
    const arrow::StructBuilder* builder, const std::string& column_name);

};  // namespace helpers
};  // namespace parquetwriter
