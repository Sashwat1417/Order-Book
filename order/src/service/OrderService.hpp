#pragma once

#include "../db/OrderRepository.hpp"
#include <nlohmann/json.hpp>
#include <string>

// Java analogy: equivalent to a Spring @Service class.
// V2: no longer holds a KafkaProducer — publishing is handled by
// the OutboxRelay background thread after the DB transaction commits.
class OrderService {
public:
    explicit OrderService(OrderRepository& repository);

    // Returns the created order as JSON, or throws on validation failure
    nlohmann::json placeOrder(const nlohmann::json& body);

    // Returns { bids: [...], asks: [...] }
    nlohmann::json getOrderBook();

    // Returns true if cancelled, false if not found
    bool cancelOrder(const std::string& id);

private:
    OrderRepository& repository_;
};
