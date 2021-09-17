#include "logging.h"

namespace parquetwriter {
namespace logging {

std::string computeMethodName(const std::string& function,
                              const std::string& pretty_function) {
    size_t locFunName = pretty_function.find(
        function);  // If the input is a constructor, it gets the beginning of
                    // the class name, not of the method. That's why later on we
                    // have to search for the first parenthesys
    size_t begin = pretty_function.rfind(" ", locFunName) + 1;
    size_t end = pretty_function.find(
        "(", locFunName + function.length());  // Adding function.length() make
                                               // this faster and also allows to
                                               // handle operator parenthesys!
    if (pretty_function[end + 1] == ')') {
        return (pretty_function.substr(begin, end - begin) + "()");
    } else {
        return (pretty_function.substr(begin, end - begin) + "(...)");
    }
}
std::shared_ptr<spdlog::logger> get_logger() {
    std::string name = "parquet-writer";
    if (auto log = spdlog::get(name); log == nullptr) {
        auto tmp = spdlog::stdout_color_mt(name);
        tmp->set_pattern("[parquet-writer] [%^%l%$] %v");
        return tmp;
    } else {
        return log;
    }
}

void set_debug() { spdlog::set_level(spdlog::level::debug); }

};  // namespace logging
};  // namespace parquetwriter
