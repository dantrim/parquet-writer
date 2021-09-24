#ifndef PARQUETWRITER_VISITOR_H
#define PARQUETWRITER_VISITOR_H

#include <iostream>

#include "parquet_helpers.h"
#include "parquet_writer_exceptions.h"

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

namespace parquetwriter {
namespace internal {

struct DataValueFillVisitor {
    DataValueFillVisitor(const std::string& field_name,
                         arrow::ArrayBuilder* builder)
        : _field_name(field_name), _builder(builder) {}

    //
    // value types
    //
    void operator()(const bool& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 0)
        helpers::fill<bool>(val, _builder);
    }
    void operator()(const uint8_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 0)
        helpers::fill<uint8_t>(val, _builder);
    }

    void operator()(const uint16_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 0)
        helpers::fill<uint16_t>(val, _builder);
    }

    void operator()(const uint32_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 0)
        helpers::fill<uint32_t>(val, _builder);
    }

    void operator()(const uint64_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 0)
        helpers::fill<uint64_t>(val, _builder);
    }

    void operator()(const int8_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 0)
        helpers::fill<int8_t>(val, _builder);
    }

    void operator()(const int16_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 0)
        helpers::fill<int16_t>(val, _builder);
    }

    void operator()(const int32_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 0)
        helpers::fill<int32_t>(val, _builder);
    }

    void operator()(const int64_t& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 0)
        helpers::fill<int64_t>(val, _builder);
    }

    void operator()(const float& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 0)
        helpers::fill<float>(val, _builder);
    }

    void operator()(const double& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 0)
        helpers::fill<double>(val, _builder);
    }

    void operator()(const std::vector<bool>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 1)
        helpers::fill<std::vector<bool>>(val, _builder);
    }

    void operator()(const std::vector<uint8_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 1)
        helpers::fill<std::vector<uint8_t>>(val, _builder);
    }

    void operator()(const std::vector<uint16_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 1)
        helpers::fill<std::vector<uint16_t>>(val, _builder);
    }

    void operator()(const std::vector<uint32_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 1)
        helpers::fill<std::vector<uint32_t>>(val, _builder);
    }

    void operator()(const std::vector<uint64_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 1)
        helpers::fill<std::vector<uint64_t>>(val, _builder);
    }

    void operator()(const std::vector<int8_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 1)
        helpers::fill<std::vector<int8_t>>(val, _builder);
    }

    void operator()(const std::vector<int16_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 1)
        helpers::fill<std::vector<int16_t>>(val, _builder);
    }

    void operator()(const std::vector<int32_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 1)
        helpers::fill<std::vector<int32_t>>(val, _builder);
    }

    void operator()(const std::vector<int64_t>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 1)
        helpers::fill<std::vector<int64_t>>(val, _builder);
    }

    void operator()(const std::vector<float>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 1)
        helpers::fill<std::vector<float>>(val, _builder);
    }

    void operator()(const std::vector<double>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 1)
        helpers::fill<std::vector<double>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<bool>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 2)
        helpers::fill<std::vector<std::vector<bool>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<uint8_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 2)
        helpers::fill<std::vector<std::vector<uint8_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<uint16_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 2)
        helpers::fill<std::vector<std::vector<uint16_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<uint32_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 2)
        helpers::fill<std::vector<std::vector<uint32_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<uint64_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 2)
        helpers::fill<std::vector<std::vector<uint64_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<int8_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 2)
        helpers::fill<std::vector<std::vector<int8_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<int16_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 2)
        helpers::fill<std::vector<std::vector<int16_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<int32_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 2)
        helpers::fill<std::vector<std::vector<int32_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<int64_t>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 2)
        helpers::fill<std::vector<std::vector<int64_t>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<float>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 2)
        helpers::fill<std::vector<std::vector<float>>>(val, _builder);
    }

    void operator()(const std::vector<std::vector<double>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 2)
        helpers::fill<std::vector<std::vector<double>>>(val, _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<bool>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, BOOL, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<bool>>>>(val,
                                                                   _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<uint8_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT8, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<uint8_t>>>>(val,
                                                                      _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<uint16_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT16, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<uint16_t>>>>(
            val, _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<uint32_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT32, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<uint32_t>>>>(
            val, _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<uint64_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, UINT64, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<uint64_t>>>>(
            val, _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<int8_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT8, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<int8_t>>>>(val,
                                                                     _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<int16_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT16, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<int16_t>>>>(val,
                                                                      _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<int32_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT32, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<int32_t>>>>(val,
                                                                      _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<int64_t>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, INT64, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<int64_t>>>>(val,
                                                                      _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<float>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, FLOAT, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<float>>>>(val,
                                                                    _builder);
    }

    void operator()(
        const std::vector<std::vector<std::vector<double>>>& val) const {
        THROW_FOR_INVALID_TYPE(_field_name, DOUBLE, _builder, 3)
        helpers::fill<std::vector<std::vector<std::vector<double>>>>(val,
                                                                     _builder);
    }

 private:
    std::string _field_name;
    arrow::ArrayBuilder* _builder;

};  // struct DataValueFillVisitor
};  // namespace internal
};  // namespace parquetwriter

#endif
