#pragma once

#include "parquet_writer_fill_types.h"

#include <iostream>
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

#define WRITER_CHECK_RESULT(expression) \
    if (!expression.ok()) throw std::logic_error(#expression);

// macro for filling 1d lists (vector<...>)
#define LIST_APPEND(LIST_BUILDER, TEMPLATE_TYPE, TYPE_CLASS, CPP_CLASS) \
    if constexpr (std::is_same<TEMPLATE_TYPE, CPP_CLASS>::value) {      \
        auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(            \
            LIST_BUILDER->value_builder());                             \
        PARQUET_THROW_NOT_OK(vb->AppendValues(val));                    \
    } else

        //auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(LIST_BUILDER); \

// macro for filling 2d lists (vector<vector<...>>)
#define INNERLIST_APPEND(LIST_BUILDER, TEMPLATE_TYPE, TYPE_CLASS, CPP_CLASS) \
    if constexpr (std::is_same<TEMPLATE_TYPE, CPP_CLASS>::value) {           \
        auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(LIST_BUILDER); \
        for(size_t i = 0; i < val.size(); i++) { \
            PARQUET_THROW_NOT_OK(vb->AppendValues(val.at(i))); \
        } \
    } else

        //for (auto v : val) {                                                 \
        //    PARQUET_THROW_NOT_OK(LIST_BUILDER->Append());                    \
        //    PARQUET_THROW_NOT_OK(vb->AppendValues(v));                       \
        //}                                                                    \

        //auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(                 \
        //    LIST_BUILDER->value_builder());                                  \

// macro for filling 3d lists (vector<vector<vector<...>>>)
#define INNERINNERLIST_APPEND(LIST_BUILDER, INNERLIST_BUILDER, TEMPLATE_TYPE, \
                              TYPE_CLASS, CPP_CLASS)                          \
    if constexpr (std::is_same<TEMPLATE_TYPE, CPP_CLASS>::value) {            \
        auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(LIST_BUILDER); \
        for(size_t i = 0; i < val.size(); i++) { \
            for(size_t j = 0; j < val.at(i).size(); j++) { \
                PARQUET_THROW_NOT_OK(vb->AppendValues(val.at(i).at(j))); \
            } \
        } \
    } else

        //auto vb = dynamic_cast<arrow::TYPE_CLASS##Builder*>(                  \
        //    INNERLIST_BUILDER->value_builder());                              \
        //for (auto v : val) {                                                  \
        //    PARQUET_THROW_NOT_OK(LIST_BUILDER->Append());                     \
        //    for (auto v2 : v) {                                               \
        //        PARQUET_THROW_NOT_OK(INNERLIST_BUILDER->Append());            \
        //        PARQUET_THROW_NOT_OK(vb->AppendValues(v2));                   \
        //    }                                                                 \
        //}                                                                     \

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

class ColumnWrapper {
 public:
    ColumnWrapper(std::string name);
    ~ColumnWrapper() = default;
    arrow::ArrayBuilder* builder() { return _builder; }
    const std::string name() { return _name; }
    void create_builder(std::shared_ptr<arrow::DataType> type);
    // void finish(std::vector<std::shared_ptr<arrow::Array>> array_vec);

 private:
    std::string _name;
    arrow::ArrayBuilder* _builder;

};  // class Node

std::shared_ptr<arrow::DataType> datatype_from_string(
    const std::string& type_string);
std::vector<std::shared_ptr<arrow::Field>> columns_from_json(
    const json& jlayout, const std::string& current_node = "");

std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
col_builder_map_from_fields(
    const std::vector<std::shared_ptr<arrow::Field>>& fields);

std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
fill_field_builder_map_from_columns(
        const std::vector<std::shared_ptr<arrow::Field>>& columns);

parquetwriter::FillType fill_type_from_column_builder(arrow::ArrayBuilder* column_builder);

bool validate_sub_struct_layout(arrow::StructBuilder* struct_builder, const std::string& parent_column_name);

std::map<std::string, arrow::ArrayBuilder*> makeVariableMap(
    std::shared_ptr<ColumnWrapper> node);
void makeVariableMap(arrow::ArrayBuilder* builder, std::string parentname,
                     std::string prefix,
                     std::map<std::string, arrow::ArrayBuilder*>& out_map);

std::pair<unsigned, unsigned> field_nums_from_struct(
    const arrow::StructBuilder* builder, const std::string& column_name);

template <typename>
struct is_std_vector : std::false_type {};

template <typename T, typename A>
struct is_std_vector<std::vector<T, A>> : std::true_type {};

template <typename T>
struct getType {
    typedef T type;
};

template <typename T, typename A>
struct getType<std::vector<T, A>> {
    typedef T type;
};

//
// primary builder fill method
//
template <typename T>
void fill(T val, arrow::ArrayBuilder* builder) {
    if constexpr (std::is_same<T, bool>::value) {
        auto vb = dynamic_cast<arrow::BooleanBuilder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, uint8_t>::value) {
        auto vb = dynamic_cast<arrow::UInt8Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, uint16_t>::value) {
        auto vb = dynamic_cast<arrow::UInt16Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, uint32_t>::value) {
        auto vb = dynamic_cast<arrow::UInt32Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, uint64_t>::value) {
        auto vb = dynamic_cast<arrow::UInt64Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, int8_t>::value) {
        auto vb = dynamic_cast<arrow::Int8Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, int16_t>::value) {
        auto vb = dynamic_cast<arrow::Int16Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, int32_t>::value) {
        auto vb = dynamic_cast<arrow::Int32Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, int64_t>::value) {
        auto vb = dynamic_cast<arrow::Int64Builder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, float>::value) {
        auto vb = dynamic_cast<arrow::FloatBuilder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (std::is_same<T, double>::value) {
        auto vb = dynamic_cast<arrow::DoubleBuilder*>(builder);
        PARQUET_THROW_NOT_OK(vb->Append(val));
    } else if constexpr (is_std_vector<T>::value) {
        std::cout << "HELPERS FILL " << __LINE__ << std::endl;
        //auto list_builder = builder;
        auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
        //std::cout << "HELPERS FILL " << __LINE__ << ", list_builder = " << list_builder << ", builder type = " << builder->type()->name() << std::endl;
        std::cout << "HELPERS FILL " << __LINE__ << " builder type = " << builder->type()->name() << std::endl;
        PARQUET_THROW_NOT_OK(list_builder->Append());
        std::cout << "HELPERS FILL " << __LINE__ << std::endl;
        typedef typename getType<T>::type InnerType;
        LIST_APPEND(list_builder, InnerType, Boolean, bool)
        LIST_APPEND(list_builder, InnerType, UInt8, uint8_t)
        LIST_APPEND(list_builder, InnerType, UInt16, uint16_t)
        LIST_APPEND(list_builder, InnerType, UInt32, uint32_t)
        LIST_APPEND(list_builder, InnerType, UInt64, uint64_t)
        LIST_APPEND(list_builder, InnerType, Int8, int8_t)
        LIST_APPEND(list_builder, InnerType, Int16, int16_t)
        LIST_APPEND(list_builder, InnerType, Int32, int32_t)
        LIST_APPEND(list_builder, InnerType, Int64, int64_t)
        LIST_APPEND(list_builder, InnerType, Float, float)
        LIST_APPEND(list_builder, InnerType, Double, double)
        if constexpr (is_std_vector<InnerType>::value) {
            auto list2_builder = builder;
            //auto list2_builder = dynamic_cast<arrow::ListBuilder*>(
            //    list_builder->value_builder());
            typedef typename getType<InnerType>::type InnerType2;
            INNERLIST_APPEND(list2_builder, InnerType2, Boolean, bool)
            INNERLIST_APPEND(list2_builder, InnerType2, UInt8, uint8_t)
            INNERLIST_APPEND(list2_builder, InnerType2, UInt16, uint16_t)
            INNERLIST_APPEND(list2_builder, InnerType2, UInt32, uint32_t)
            INNERLIST_APPEND(list2_builder, InnerType2, UInt64, uint64_t)
            INNERLIST_APPEND(list2_builder, InnerType2, Int8, int8_t)
            INNERLIST_APPEND(list2_builder, InnerType2, Int16, int16_t)
            INNERLIST_APPEND(list2_builder, InnerType2, Int32, int32_t)
            INNERLIST_APPEND(list2_builder, InnerType2, Int64, int64_t)
            INNERLIST_APPEND(list2_builder, InnerType2, Float, float)
            INNERLIST_APPEND(list2_builder, InnerType2, Double, double)
            if constexpr (is_std_vector<InnerType2>::value) {
                //auto list3_builder = dynamic_cast<arrow::ListBuilder*>(
                //    list2_builder->value_builder());
                auto list3_builder = builder;
                typedef typename getType<InnerType2>::type InnerType3;
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Boolean, bool)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      UInt8, uint8_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      UInt16, uint16_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      UInt32, uint32_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      UInt64, uint64_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Int8, int8_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Int16, int16_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Int32, int32_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Int64, int64_t)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Float, float)
                INNERINNERLIST_APPEND(list2_builder, list3_builder, InnerType3,
                                      Double, double) {
                    throw std::logic_error(
                        "ERROR: Unsupported list (dim=3) type");
                }
            } else {
                throw std::logic_error("ERROR: Unsupported list (dim=2) type");
            }
        } else {
            throw std::logic_error(
                "ERROR: Invalid data type for helpers::fill<std::vector>");
        }
    }  // std::vector
    else {
        throw std::logic_error("ERROR: Invalid data type for helpers::fill");
    }
}  // fill

};  // namespace helpers
};  // namespace parquetwriter
