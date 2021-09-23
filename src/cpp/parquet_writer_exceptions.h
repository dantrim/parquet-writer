#ifndef PARQUETWRITER_EXCEPTIONS_H
#define PARQUETWRITER_EXCEPTIONS_H

#include <stdexcept>

namespace parquetwriter {

// base exception
class writer_exception : public std::runtime_error {
 public:
    writer_exception(std::string what);
};

class not_implemented_exception : public std::logic_error {
 public:
    not_implemented_exception(std::string what);
};

// thrown if parsing of file layout goes wrong
class layout_exception : public std::logic_error {
 public:
    layout_exception(std::string what);
};

// thrown if something is wrong with the data provided to "fill"
class data_buffer_exception : public std::runtime_error {
 public:
    data_buffer_exception(std::string what);
};

// thrown if there is a mis-match between provided and expected data types
class data_type_exception : public std::runtime_error {
 public:
    data_type_exception(std::string what);
};

};  // namespace parquetwriter

#endif
