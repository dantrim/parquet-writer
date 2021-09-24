#ifndef PARQUETWRITER_VISITOR_H
#define PARQUETWRITER_VISITOR_H

#include "parquet_writer_exceptions.h"
#include "parquet_writer_helpers.h"

#define THROW_FOR_INVALID_TYPE(FIELDNAME, ARROWTYPE, ARRAYBUILDER, LISTDEPTH)  \
    if (ARRAYBUILDER->type()->id() != arrow::Type::ARROWTYPE) {                \
        if (ARRAYBUILDER->type()->id() == arrow::Type::LIST ||                 \
            LISTDEPTH > 0) {                                                   \
            arrow::ArrayBuilder* expected_builder = nullptr;                   \
            unsigned depth = 0;                                                \
            if (ARRAYBUILDER->type()->id() == arrow::Type::LIST) {             \
                auto list_builder =                                            \
                    dynamic_cast<arrow::ListBuilder*>(ARRAYBUILDER);           \
                std::tie(depth, expected_builder) =                            \
                    helpers::list_builder_description(list_builder);           \
            } else {                                                           \
                expected_builder = ARRAYBUILDER;                               \
            }                                                                  \
            if (expected_builder->type()->id() != arrow::Type::ARROWTYPE ||    \
                depth != LISTDEPTH) {                                          \
                std::string expect_type =                                      \
                    (depth > 0 ? "list" + std::to_string(depth) + "d[" +       \
                                     expected_builder->type()->name() + "]"    \
                               : ARRAYBUILDER->type()->name());                \
                std::string got_type =                                         \
                    (LISTDEPTH > 0 ? "list" + std::to_string(LISTDEPTH) +      \
                                         "d[" + #ARROWTYPE + "]"               \
                                   : #ARROWTYPE);                              \
                throw parquetwriter::data_type_exception(                      \
                    "Invalid data type provided for column/field \"" +         \
                    FIELDNAME + "\", expect: \"" + expect_type +               \
                    "\", got: \"" + got_type + "\"");                          \
            }                                                                  \
        } else {                                                               \
            throw parquetwriter::data_type_exception(                          \
                "Invalid data type provided for column/field \"" + FIELDNAME + \
                "\", expect: \"" + ARRAYBUILDER->type()->name() +              \
                "\", got: \"" + #ARROWTYPE + "\"");                            \
        }                                                                      \
    }

#define VALUE_APPEND(ARRAYBUILDER, VALUEBUILDERCLASS)                    \
    auto value_builder = dynamic_cast<VALUEBUILDERCLASS*>(ARRAYBUILDER); \
    PARQUET_THROW_NOT_OK(value_builder->Append(val));

// macro for filling 1d lists (vector<...>)
#define LIST1D_APPEND(ARRAYBUILDER, VALUEBUILDERCLASS)                   \
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(ARRAYBUILDER); \
    auto value_builder =                                                 \
        dynamic_cast<VALUEBUILDERCLASS*>(list_builder->value_builder()); \
    PARQUET_THROW_NOT_OK(list_builder->Append());                        \
    PARQUET_THROW_NOT_OK(value_builder->AppendValues(val));

// macro for filling 2d lists (vector<vector<...>>)
#define LIST2D_APPEND(ARRAYBUILDER, VALUEBUILDERCLASS)                    \
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(ARRAYBUILDER);  \
    PARQUET_THROW_NOT_OK(list_builder->Append());                         \
    list_builder =                                                        \
        dynamic_cast<arrow::ListBuilder*>(list_builder->value_builder()); \
    auto value_builder =                                                  \
        dynamic_cast<VALUEBUILDERCLASS*>(list_builder->value_builder());  \
    for (size_t i = 0; i < val.size(); i++) {                             \
        PARQUET_THROW_NOT_OK(list_builder->Append());                     \
        PARQUET_THROW_NOT_OK(value_builder->AppendValues(val.at(i)));     \
    }

// macro for filling 3d lists (vector<vector<vector<...>>>)
#define LIST3D_APPEND(ARRAYBUILDER, VALUEBUILDERCLASS)                    \
    auto list_builder = dynamic_cast<arrow::ListBuilder*>(ARRAYBUILDER);  \
    auto inner_list_builder =                                             \
        dynamic_cast<arrow::ListBuilder*>(list_builder->value_builder()); \
    auto inner_inner_list_builder = dynamic_cast<arrow::ListBuilder*>(    \
        inner_list_builder->value_builder());                             \
    auto value_builder = dynamic_cast<VALUEBUILDERCLASS*>(                \
        inner_inner_list_builder->value_builder());                       \
    PARQUET_THROW_NOT_OK(list_builder->Append());                         \
    for (size_t i = 0; i < val.size(); i++) {                             \
        PARQUET_THROW_NOT_OK(inner_list_builder->Append());               \
        for (size_t j = 0; j < val.at(i).size(); j++) {                   \
            PARQUET_THROW_NOT_OK(inner_inner_list_builder->Append());     \
            PARQUET_THROW_NOT_OK(                                         \
                value_builder->AppendValues(val.at(i).at(j)));            \
        }                                                                 \
    }

namespace parquetwriter {
namespace internal {

template <class>
inline constexpr bool missing_type_impl_for = false;

struct DataValueFillVisitor {
    DataValueFillVisitor(const std::string& field_name,
                         arrow::ArrayBuilder* builder)
        : _field_name(field_name), _builder(builder) {}

    // make a visitor that can be checked for exhaustively handling
    // all variant types at compile-time
    template <class type>
    void operator()(type val) {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, bool>) {
            THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 0)
            VALUE_APPEND(_builder, arrow::BooleanBuilder)
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 0)
            VALUE_APPEND(_builder, arrow::UInt8Builder)
        } else if constexpr (std::is_same_v<T, uint16_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 0)
            VALUE_APPEND(_builder, arrow::UInt16Builder)
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 0)
            VALUE_APPEND(_builder, arrow::UInt32Builder)
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 0)
            VALUE_APPEND(_builder, arrow::UInt64Builder)
        } else if constexpr (std::is_same_v<T, int8_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 0)
            VALUE_APPEND(_builder, arrow::Int8Builder)
        } else if constexpr (std::is_same_v<T, int16_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 0)
            VALUE_APPEND(_builder, arrow::Int16Builder)
        } else if constexpr (std::is_same_v<T, int32_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 0)
            VALUE_APPEND(_builder, arrow::Int32Builder)
        } else if constexpr (std::is_same_v<T, int64_t>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 0)
            VALUE_APPEND(_builder, arrow::Int64Builder)
        } else if constexpr (std::is_same_v<T, float>) {
            THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 0)
            VALUE_APPEND(_builder, arrow::FloatBuilder)
        } else if constexpr (std::is_same_v<T, double>) {
            THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 0)
            VALUE_APPEND(_builder, arrow::DoubleBuilder)
        } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
            THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 1)
            LIST1D_APPEND(_builder, arrow::BooleanBuilder)
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 1)
            LIST1D_APPEND(_builder, arrow::UInt8Builder)
        } else if constexpr (std::is_same_v<T, std::vector<uint16_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 1)
            LIST1D_APPEND(_builder, arrow::UInt16Builder)
        } else if constexpr (std::is_same_v<T, std::vector<uint32_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 1)
            LIST1D_APPEND(_builder, arrow::UInt32Builder)
        } else if constexpr (std::is_same_v<T, std::vector<uint64_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 1)
            LIST1D_APPEND(_builder, arrow::UInt64Builder)
        } else if constexpr (std::is_same_v<T, std::vector<int8_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 1)
            LIST1D_APPEND(_builder, arrow::Int8Builder)
        } else if constexpr (std::is_same_v<T, std::vector<int16_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 1)
            LIST1D_APPEND(_builder, arrow::Int16Builder)
        } else if constexpr (std::is_same_v<T, std::vector<int32_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 1)
            LIST1D_APPEND(_builder, arrow::Int32Builder)
        } else if constexpr (std::is_same_v<T, std::vector<int64_t>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 1)
            LIST1D_APPEND(_builder, arrow::Int64Builder)
        } else if constexpr (std::is_same_v<T, std::vector<float>>) {
            THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 1)
            LIST1D_APPEND(_builder, arrow::FloatBuilder)
        } else if constexpr (std::is_same_v<T, std::vector<double>>) {
            THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 1)
            LIST1D_APPEND(_builder, arrow::DoubleBuilder)
        } else if constexpr (std::is_same_v<T,
                                            std::vector<std::vector<bool>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 2)
            LIST2D_APPEND(_builder, arrow::BooleanBuilder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<uint8_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 2)
            LIST2D_APPEND(_builder, arrow::UInt8Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<uint16_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 2)
            LIST2D_APPEND(_builder, arrow::UInt16Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<uint32_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 2)
            LIST2D_APPEND(_builder, arrow::UInt32Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<uint64_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 2)
            LIST2D_APPEND(_builder, arrow::UInt64Builder)
        } else if constexpr (std::is_same_v<T,
                                            std::vector<std::vector<int8_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 2)
            LIST2D_APPEND(_builder, arrow::Int8Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<int16_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 2)
            LIST2D_APPEND(_builder, arrow::Int16Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<int32_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 2)
            LIST2D_APPEND(_builder, arrow::Int32Builder)
        } else if constexpr (std::is_same_v<
                                 T, std::vector<std::vector<int64_t>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 2)
            LIST2D_APPEND(_builder, arrow::Int64Builder)
        } else if constexpr (std::is_same_v<T,
                                            std::vector<std::vector<float>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 2)
            LIST2D_APPEND(_builder, arrow::FloatBuilder)
        } else if constexpr (std::is_same_v<T,
                                            std::vector<std::vector<double>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 2)
            LIST2D_APPEND(_builder, arrow::DoubleBuilder)
        } else if constexpr (std::is_same_v<
                                 T,
                                 std::vector<std::vector<std::vector<bool>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 3)
            LIST3D_APPEND(_builder, arrow::BooleanBuilder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<uint8_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 3)
            LIST3D_APPEND(_builder, arrow::UInt8Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<uint16_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 3)
            LIST3D_APPEND(_builder, arrow::UInt16Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<uint32_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 3)
            LIST3D_APPEND(_builder, arrow::UInt32Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<uint64_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 3)
            LIST3D_APPEND(_builder, arrow::UInt64Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<int8_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 3)
            LIST3D_APPEND(_builder, arrow::Int8Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<int16_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 3)
            LIST3D_APPEND(_builder, arrow::Int16Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<int32_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 3)
            LIST3D_APPEND(_builder, arrow::Int32Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<int64_t>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 3)
            LIST3D_APPEND(_builder, arrow::Int64Builder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<float>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 3)
            LIST3D_APPEND(_builder, arrow::FloatBuilder)
        } else if constexpr (std::is_same_v<T, std::vector<std::vector<
                                                   std::vector<double>>>>) {
            THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 3)
            LIST3D_APPEND(_builder, arrow::DoubleBuilder)
        } else {
            static_assert(missing_type_impl_for<T>,
                          "Data filling visitor is non-exhaustive");
        }
    }

 private:
    std::string _field_name;
    arrow::ArrayBuilder* _builder;

};  // struct DataValueFillVisitor
};  // namespace internal
};  // namespace parquetwriter

#endif
