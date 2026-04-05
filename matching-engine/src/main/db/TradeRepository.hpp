#pragma once

#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <string>

#include <vector>
#include "Trade.hpp"
#include "Order.hpp"

// Java analogy: like a @Repository with three MongoCollections injected —
// one for writing trades and two for updating order status.
class TradeRepository {
public:
    explicit TradeRepository(mongocxx::database& db);

    // Session-aware overloads — used inside a transaction for atomic trade persistence.
    // Java analogy: passing an EntityManager/Session explicitly for transactional context.
    void insertTrade(mongocxx::client_session& session, const Trade& trade);
    void updateOrderStatus(mongocxx::client_session& session,
                           const std::string& orderId,
                           const std::string& status,
                           double remaining);

    // Returns all OPEN + PARTIALLY_FILLED orders from both collections.
    // Used to seed the in-memory book on startup.
    std::vector<Order> findOpenOrders();

    // Returns order IDs (any status) with timestamp >= sinceTimestampMs.
    // Used to pre-warm Redis dedupe on startup to skip Kafka replays after restart.
    std::vector<std::string> findOrderIdsSince(int64_t sinceTimestampMs);

private:
    mongocxx::collection trades_;
    mongocxx::collection bids_;
    mongocxx::collection asks_;
};
