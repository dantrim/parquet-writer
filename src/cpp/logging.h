#ifndef PARQUETWRITER_LOGGING_H
#define PARQUETWRITER_LOGGING_H

#include <memory>

#include "spdlog/logger.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace parquetwriter {
namespace logging {

std::string computeMethodName(const std::string& function,
                              const std::string& pretty_function);
#define __PRETTYFUNCTION__                                         \
    parquetwriter::logging::computeMethodName(__FUNCTION__,        \
                                              __PRETTY_FUNCTION__) \
        .c_str()  // c_str() is optional

std::shared_ptr<spdlog::logger> get_logger();

void set_debug();

};  // namespace logging
};  // namespace parquetwriter

#endif
