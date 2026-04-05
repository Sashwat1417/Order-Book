#pragma once

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

    void insertTrade(const Trade& trade);

    // Updates status + remaining on whichever collection (bids/asks) holds the order.
    // Both are updated; at most one will match.
    void updateOrderStatus(const std::string& orderId,
                           const std::string& status,
                           double remaining);

    // Returns all OPEN + PARTIALLY_FILLED orders from both collections.
    // Used to seed the in-memory book on startup.
    std::vector<Order> findOpenOrders();

private:
    mongocxx::collection trades_;
    mongocxx::collection bids_;
    mongocxx::collection asks_;
};
