#ifndef PARQUETWRITER_FILL_TYPES_H
#define PARQUETWRITER_FILL_TYPES_H

namespace parquetwriter {

    // supported types for a given column in the
    // output Parquet files
    enum class FillType {
        VALUE
        ,VALUE_LIST_1D
        ,VALUE_LIST_2D
        ,VALUE_LIST_3D
        ,STRUCT
        ,STRUCT_LIST_1D
        ,STRUCT_LIST_2D
        ,STRUCT_LIST_3D
        ,INVALID
    }; // enum FillTypes

}; // namespace parquetwriter

#endif
