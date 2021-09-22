#include "parquet_writer_fill_types.h"

namespace parquetwriter {

    std::string filltype_to_string(FillType fill_type) {
        std::string out = "INVALID";
        switch(fill_type) {
            case FillType::VALUE :
                out = "VALUE";
                break;
            case FillType::VALUE_LIST_1D :
                out = "VALUE_LIST_1D";
                break;
            case FillType::VALUE_LIST_2D :
                out = "VAUE_LIST_2D";
                break;
            case FillType::VALUE_LIST_3D :
                out = "VALUE_LIST_3D";
                break;
            case FillType::STRUCT :
                out = "STRUCT";
                break;
            case FillType::STRUCT_LIST_1D :
                out = "STRUCT_LIST_1D";
                break;
            case FillType::STRUCT_LIST_2D :
                out = "STRUCT_LIST_2D";
                break;
            case FillType::STRUCT_LIST_3D :
                out = "STRUCT_LIST_3D";
                break;
            default :
                out = "INVALID";
                break;
        } // switch
        return out;
    }

}; // namespace parquetwriter
