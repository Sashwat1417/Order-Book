#pragma once

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>

enum class Side {
    BID,
    ASK
};

enum class OrderStatus {
    OPEN,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED
};

struct Order {
    std::string  id;
    Side         side;
    double       price;
    double       quantity;
    double       remaining;
    OrderStatus  status;
    int64_t      timestamp;  // Unix ms

    static Order fromJson(const nlohmann::json& j) {
        Order o;
        o.id        = j.at("id").get<std::string>();
        o.price     = j.at("price").get<double>();
        o.quantity  = j.at("quantity").get<double>();
        o.remaining = j.value("remaining", o.quantity);
        o.timestamp = j.at("timestamp").get<int64_t>();

        std::string side = j.at("side").get<std::string>();
        o.side = (side == "BID") ? Side::BID : Side::ASK;

        std::string status = j.value("status", std::string("OPEN"));
        if      (status == "PARTIALLY_FILLED") o.status = OrderStatus::PARTIALLY_FILLED;
        else if (status == "FILLED")           o.status = OrderStatus::FILLED;
        else if (status == "CANCELLED")        o.status = OrderStatus::CANCELLED;
        else                                   o.status = OrderStatus::OPEN;

        return o;
    }

    nlohmann::json toJson() const {
        return {
            {"id",        id},
            {"side",      side == Side::BID ? "BID" : "ASK"},
            {"price",     price},
            {"quantity",  quantity},
            {"remaining", remaining},
            {"status",    statusToString()},
            {"timestamp", timestamp}
        };
    }

    std::string statusToString() const {
        switch (status) {
            case OrderStatus::OPEN:             return "OPEN";
            case OrderStatus::PARTIALLY_FILLED: return "PARTIALLY_FILLED";
            case OrderStatus::FILLED:           return "FILLED";
            case OrderStatus::CANCELLED:        return "CANCELLED";
        }
        return "UNKNOWN";
    }
};
