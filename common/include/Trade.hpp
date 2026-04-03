#pragma once

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>

struct Trade {
    std::string id;
    std::string bidOrderId;
    std::string askOrderId;
    double      price;
    double      quantity;
    int64_t     timestamp;  // Unix ms

    nlohmann::json toJson() const {
        return {
            {"id",         id},
            {"bidOrderId", bidOrderId},
            {"askOrderId", askOrderId},
            {"price",      price},
            {"quantity",   quantity},
            {"timestamp",  timestamp}
        };
    }
};
