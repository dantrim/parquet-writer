#include "parquet_writer_exceptions.h"

#include <string>

namespace parquetwriter {

writer_exception::writer_exception(std::string what)
    : std::runtime_error(what) {}

not_implemented_exception::not_implemented_exception(std::string what)
    : std::logic_error(what) {}

layout_exception::layout_exception(std::string what) : std::logic_error(what) {}

data_buffer_exception::data_buffer_exception(std::string what)
    : std::runtime_error(what) {}

data_type_exception::data_type_exception(std::string what)
    : std::runtime_error(what) {}

};  // namespace parquetwriter
