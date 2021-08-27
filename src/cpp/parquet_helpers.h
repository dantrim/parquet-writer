#pragma once

//std/stl
#include <string>
#include <vector>
#include <memory>
#include <map>

//arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/exception.h>
#include <arrow/type.h> // struct_

// json
#include "nlohmann/json.hpp"

#define WRITER_CHECK_RESULT(expression) \
    if (!expression.ok()) \
        throw std::logic_error(#expression);

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
            type_ptr get_float16() { return arrow::float16(); }
            type_ptr get_float32() { return arrow::float32(); }
            type_ptr get_float64() { return arrow::float64(); }
            type_ptr get_string() { return arrow::utf8(); }
                
        }; // ArrowTypeInit

        typedef type_ptr(ArrowTypeInit::*TypeInitFunc)(void);

        static std::map<std::string, TypeInitFunc> const type_init_map = {
        	{"bool",  &parquetwriter::helpers::internal::ArrowTypeInit::get_bool},
        	{"int8",  &parquetwriter::helpers::internal::ArrowTypeInit::get_int8},
        	{"int16",  &parquetwriter::helpers::internal::ArrowTypeInit::get_int16},
        	{"int32",  &parquetwriter::helpers::internal::ArrowTypeInit::get_int32},
        	{"int64",  &parquetwriter::helpers::internal::ArrowTypeInit::get_int64},
        	{"uint8",  &parquetwriter::helpers::internal::ArrowTypeInit::get_uint8},
        	{"uint16",  &parquetwriter::helpers::internal::ArrowTypeInit::get_uint16},
        	{"uint32",  &parquetwriter::helpers::internal::ArrowTypeInit::get_uint32},
        	{"uint64",  &parquetwriter::helpers::internal::ArrowTypeInit::get_uint64},
        	{"float16",  &parquetwriter::helpers::internal::ArrowTypeInit::get_float16},
        	{"float32",  &parquetwriter::helpers::internal::ArrowTypeInit::get_float32},
        	{"float64",  &parquetwriter::helpers::internal::ArrowTypeInit::get_float64},
        	{"string",  &parquetwriter::helpers::internal::ArrowTypeInit::get_string}
		};

    }; // internal

    using nlohmann::json;

    class ColumnWrapper {
        public :
            ColumnWrapper(std::string name);
            ~ColumnWrapper() = default;
            arrow::ArrayBuilder* builder() { return _builder; }
            const std::string name() { return _name; }
            void create_builder(std::shared_ptr<arrow::DataType> type);
            //void finish(std::vector<std::shared_ptr<arrow::Array>> array_vec);

        private :
            std::string _name;
            arrow::ArrayBuilder* _builder;
         
    }; // class Node

    std::shared_ptr<arrow::DataType> datatype_from_string(const std::string& type_string);
    std::vector<std::shared_ptr<arrow::Field>> fields_from_json(const json& jlayout);

    std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> 
        col_builder_map_from_fields(const std::vector<std::shared_ptr<arrow::Field>>& fields);

    std::map<std::string, arrow::ArrayBuilder*> makeVariableMap(std::shared_ptr<ColumnWrapper> node);
    void makeVariableMap(arrow::ArrayBuilder* builder,
            std::string parentname,
            std::string prefix,
            std::map<std::string, arrow::ArrayBuilder*>& out_map);

    template<typename>
    struct is_std_vector : std::false_type {};

    template<typename T, typename A>
    struct is_std_vector<std::vector<T,A>> : std::true_type {};

    template<typename T>
    struct getType {
        typedef T type;
    };
    
    template <typename T, typename A>
    struct getType<std::vector<T,A>> {
        typedef T type;
    };

    //
    // primary builder fill method
    //
    template<typename T>
    void fill(T val, arrow::ArrayBuilder* builder) {
        if constexpr(std::is_same<T, bool>::value) {
            auto vb = dynamic_cast<arrow::BooleanBuilder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, uint8_t>::value) {
            auto vb = dynamic_cast<arrow::UInt8Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, uint16_t>::value) {
            auto vb = dynamic_cast<arrow::UInt16Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, uint32_t>::value) {
            auto vb = dynamic_cast<arrow::UInt32Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, uint64_t>::value) {
            auto vb = dynamic_cast<arrow::UInt64Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, int8_t>::value) {
            auto vb = dynamic_cast<arrow::Int8Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, int16_t>::value) {
            auto vb = dynamic_cast<arrow::Int16Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, int32_t>::value) {
            auto vb = dynamic_cast<arrow::Int32Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, int64_t>::value) {
            auto vb = dynamic_cast<arrow::Int64Builder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, float>::value) {
            auto vb = dynamic_cast<arrow::FloatBuilder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(std::is_same<T, double>::value) {
            auto vb = dynamic_cast<arrow::DoubleBuilder*>(builder);
            PARQUET_THROW_NOT_OK(vb->Append(val));
        } else
        if constexpr(is_std_vector<T>::value) {
            auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
            PARQUET_THROW_NOT_OK(list_builder->Append());

            typedef typename getType<T>::type InnerType;

            if constexpr(std::is_same<InnerType, bool>::value) {
                auto vb = dynamic_cast<arrow::BooleanBuilder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, uint8_t>::value) {
                auto vb = dynamic_cast<arrow::UInt8Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, uint16_t>::value) {
                auto vb = dynamic_cast<arrow::UInt16Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, uint32_t>::value) {
                auto vb = dynamic_cast<arrow::UInt32Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, uint64_t>::value) {
                auto vb = dynamic_cast<arrow::UInt64Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, int8_t>::value) {
                auto vb = dynamic_cast<arrow::Int8Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, int16_t>::value) {
                auto vb = dynamic_cast<arrow::Int16Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, int32_t>::value) {
                auto vb = dynamic_cast<arrow::Int32Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, int64_t>::value) {
                auto vb = dynamic_cast<arrow::Int64Builder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, float>::value) {
                auto vb = dynamic_cast<arrow::FloatBuilder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else
            if constexpr(std::is_same<InnerType, double>::value) {
                auto vb = dynamic_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
                PARQUET_THROW_NOT_OK(vb->AppendValues(val));
            } else {
                throw std::logic_error("ERROR: Invalid data type for helpers::fill<std::vector>");
            }
        } // std::vector
        else {
            throw std::logic_error("ERROR: Invalid data type for helpers::fill");
        }
    } // fill


}; // namespace helpers
}; // namespace parquetwriter
