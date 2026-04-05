#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <nlohmann/json.hpp>
#include <Order.hpp>
#include <string>

// Java analogy: equivalent to a Spring @Repository / MongoRepository<Order, String>
class OrderRepository {
public:
    // client is needed to start MongoDB multi-document transactions
    OrderRepository(mongocxx::client& client, mongocxx::database& db);

    // Atomically inserts order + outbox record in a single MongoDB transaction.
    // The outbox relay will pick up the outbox record and publish to Kafka.
    // Java analogy: @Transactional method calling two repository.save() calls.
    void insertOrderWithOutbox(const Order& order);

    nlohmann::json findOpenBids();   // sorted price DESC
    nlohmann::json findOpenAsks();   // sorted price ASC
    bool cancelOrder(const std::string& id);

private:
    mongocxx::client&    client_;   // needed for session/transaction
    mongocxx::collection bids_;
    mongocxx::collection asks_;
    mongocxx::collection outbox_;
};
