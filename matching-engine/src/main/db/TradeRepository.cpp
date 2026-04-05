#include "TradeRepository.hpp"

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/find.hpp>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace stream = bsoncxx::builder::stream;

TradeRepository::TradeRepository(mongocxx::database& db)
    : trades_(db["trades"])
    , bids_(db["bids"])
    , asks_(db["asks"])
{}

void TradeRepository::insertTrade(const Trade& trade) {
    try {
        auto doc = stream::document{}
            << "id"         << trade.id
            << "bidOrderId" << trade.bidOrderId
            << "askOrderId" << trade.askOrderId
            << "price"      << trade.price
            << "quantity"   << trade.quantity
            << "timestamp"  << trade.timestamp
            << stream::finalize;

        auto result = trades_.insert_one(doc.view());
        if (!result) {
            throw std::runtime_error("Trade insert not acknowledged by MongoDB");
        }
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB insertTrade failed: ") + e.what());
    }
}

void TradeRepository::updateOrderStatus(const std::string& orderId,
                                        const std::string& status,
                                        double remaining) {
    try {
        // Store docs in named variables — views must not outlive their owner
        auto filter = stream::document{} << "id" << orderId << stream::finalize;
        auto update = stream::document{}
            << "$set" << stream::open_document
                << "status"    << status
                << "remaining" << remaining
            << stream::close_document
            << stream::finalize;

        bids_.update_one(filter.view(), update.view());
        asks_.update_one(filter.view(), update.view());
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB updateOrderStatus failed: ") + e.what());
    }
}

std::vector<Order> TradeRepository::findOpenOrders() {
    std::vector<Order> orders;

    try {
        // Filter: status IN ["OPEN", "PARTIALLY_FILLED"]
        auto filter = stream::document{}
            << "status" << stream::open_document
                << "$in" << stream::open_array
                    << "OPEN" << "PARTIALLY_FILLED"
                << stream::close_array
            << stream::close_document
            << stream::finalize;

        // Sort by timestamp ASC — restores price-time priority within each price level.
        // SQL equivalent: ORDER BY timestamp ASC
        // Java analogy: findByStatusInOrderByTimestampAsc() in Spring Data
        auto sort = stream::document{} << "timestamp" << 1 << stream::finalize;

        mongocxx::options::find opts;
        opts.sort(sort.view());

        // Query bids collection — side is implicit (it's the bids collection)
        auto bidCursor = bids_.find(filter.view(), opts);
        for (auto&& doc : bidCursor) {
            auto j = nlohmann::json::parse(bsoncxx::to_json(doc));
            j["side"] = "BID";
            orders.push_back(Order::fromJson(j));
        }

        // Query asks collection — side is implicit
        auto askCursor = asks_.find(filter.view(), opts);
        for (auto&& doc : askCursor) {
            auto j = nlohmann::json::parse(bsoncxx::to_json(doc));
            j["side"] = "ASK";
            orders.push_back(Order::fromJson(j));
        }

    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB findOpenOrders failed: ") + e.what());
    }

    return orders;
}
