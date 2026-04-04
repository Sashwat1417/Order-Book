#pragma once

#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <nlohmann/json.hpp>
#include <Order.hpp>
#include <string>

// Java analogy: equivalent to a Spring @Repository / MongoRepository<Order, String>
class OrderRepository {
public:
    explicit OrderRepository(mongocxx::database& db);

    void insertOrder(const Order& order);
    nlohmann::json findOpenBids();   // sorted price DESC
    nlohmann::json findOpenAsks();   // sorted price ASC
    bool cancelOrder(const std::string& id);

private:
    mongocxx::collection bids_;
    mongocxx::collection asks_;
};
