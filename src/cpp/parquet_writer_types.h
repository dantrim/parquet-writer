#pragma once

//std/stl
#include <variant>
#include <vector>

namespace parquetwriter {
namespace types {
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
            std::vector<std::vector<std::vector<double>>>
        > buffer_value_t;

    typedef std::vector<buffer_value_t> buffer_value_vec_t;
    typedef std::variant<buffer_value_t, buffer_value_vec_t> buffer_t;
}
}; // namespace parquetwriter
