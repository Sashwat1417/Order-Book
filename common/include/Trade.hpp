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

    static Trade fromJson(const nlohmann::json& j) {
        Trade t;
        t.id         = j.at("id").get<std::string>();
        t.bidOrderId = j.at("bidOrderId").get<std::string>();
        t.askOrderId = j.at("askOrderId").get<std::string>();
        t.price      = j.at("price").get<double>();
        t.quantity   = j.at("quantity").get<double>();
        t.timestamp  = j.at("timestamp").get<int64_t>();
        return t;
    }

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
