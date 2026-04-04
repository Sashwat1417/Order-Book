#pragma once

#include "../db/OrderRepository.hpp"
#include "../kafka/KafkaProducer.hpp"
#include <nlohmann/json.hpp>
#include <string>

// Java analogy: equivalent to a Spring @Service class
class OrderService {
public:
    OrderService(OrderRepository& repository, KafkaProducer& kafkaProducer);

    // Returns the created order as JSON, or throws on validation failure
    nlohmann::json placeOrder(const nlohmann::json& body);

    // Returns { bids: [...], asks: [...] }
    nlohmann::json getOrderBook();

    // Returns true if cancelled, false if not found
    bool cancelOrder(const std::string& id);

private:
    OrderRepository& repository_;
    KafkaProducer&   kafkaProducer_;
};
