#pragma once

// std/stl
#include <stdint.h>

#include <map>
#include <variant>
#include <vector>

namespace parquetwriter {
namespace types {
// clang-format off
typedef std::variant<
    bool,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double,
    std::vector<bool>,
    std::vector<uint8_t>,
    std::vector<uint16_t>,
    std::vector<uint32_t>,
    std::vector<uint64_t>,
    std::vector<int8_t>,
    std::vector<int16_t>,
    std::vector<int32_t>,
    std::vector<int64_t>,
    std::vector<float>,
    std::vector<double>,
    std::vector<std::vector<bool>>,
    std::vector<std::vector<uint8_t>>,
    std::vector<std::vector<uint16_t>>,
    std::vector<std::vector<uint32_t>>,
    std::vector<std::vector<uint64_t>>,
    std::vector<std::vector<int8_t>>,
    std::vector<std::vector<int16_t>>,
    std::vector<std::vector<int32_t>>,
    std::vector<std::vector<int64_t>>,
    std::vector<std::vector<float>>,
    std::vector<std::vector<double>>,
    std::vector<std::vector<std::vector<bool>>>,
    std::vector<std::vector<std::vector<uint8_t>>>,
    std::vector<std::vector<std::vector<uint16_t>>>,
    std::vector<std::vector<std::vector<uint32_t>>>,
    std::vector<std::vector<std::vector<uint64_t>>>,
    std::vector<std::vector<std::vector<int8_t>>>,
    std::vector<std::vector<std::vector<int16_t>>>,
    std::vector<std::vector<std::vector<int32_t>>>,
    std::vector<std::vector<std::vector<int64_t>>>,
    std::vector<std::vector<std::vector<float>>>,
    std::vector<std::vector<std::vector<double>>>>
    buffer_value_t;
// clang-format on
};  // namespace types

// these are the only types we should have
typedef types::buffer_value_t value_t;
typedef std::vector<value_t> field_buffer_t;
typedef std::map<std::string, value_t> field_map_t;

};  // namespace parquetwriter
