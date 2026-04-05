#include "TradeRepository.hpp"

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/index.hpp>
#include <nlohmann/json.hpp>
#include <unordered_set>
#include <stdexcept>

namespace stream = bsoncxx::builder::stream;

TradeRepository::TradeRepository(mongocxx::database& db)
    : trades_(db["trades"])
    , bids_(db["bids"])
    , asks_(db["asks"])
{
    // Enforce unique constraint on the application-level "id" field for all three
    // collections. Java analogy: @Indexed(unique = true) on the id field in a
    // Spring Data @Document. createIndex is idempotent — safe to call on every startup.
    auto idIndex = stream::document{} << "id" << 1 << stream::finalize;
    mongocxx::options::index uniqueOpts;
    uniqueOpts.unique(true);

    trades_.create_index(idIndex.view(), uniqueOpts);
    bids_.create_index(idIndex.view(), uniqueOpts);
    asks_.create_index(idIndex.view(), uniqueOpts);
}

void TradeRepository::insertTrade(mongocxx::client_session& session, const Trade& trade) {
    try {
        auto doc = stream::document{}
            << "id"         << trade.id
            << "bidOrderId" << trade.bidOrderId
            << "askOrderId" << trade.askOrderId
            << "price"      << trade.price
            << "quantity"   << trade.quantity
            << "timestamp"  << trade.timestamp
            << stream::finalize;

        auto result = trades_.insert_one(session, doc.view());
        if (!result) {
            throw std::runtime_error("Trade insert not acknowledged by MongoDB");
        }
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB insertTrade failed: ") + e.what());
    }
}

void TradeRepository::updateOrderStatus(mongocxx::client_session& session,
                                        const std::string& orderId,
                                        const std::string& status,
                                        double remaining) {
    try {
        // Store docs in named variables — views must not outlive their owner
        auto filter = stream::document{} << "id" << orderId << stream::finalize;
        auto bidUpdate = stream::document{}
            << "$set" << stream::open_document
                << "status"    << status
                << "remaining" << remaining
                << "side"      << "BID"
            << stream::close_document
            << stream::finalize;
        auto askUpdate = stream::document{}
            << "$set" << stream::open_document
                << "status"    << status
                << "remaining" << remaining
                << "side"      << "ASK"
            << stream::close_document
            << stream::finalize;

        auto bidResult = bids_.update_one(session, filter.view(), bidUpdate.view());
        auto askResult = asks_.update_one(session, filter.view(), askUpdate.view());

        bool updated = (bidResult && bidResult->modified_count() > 0) ||
                       (askResult && askResult->modified_count() > 0);
        if (!updated) {
            throw std::runtime_error("updateOrderStatus: no document modified for orderId=" + orderId);
        }
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

std::vector<std::string> TradeRepository::findOrderIdsSince(int64_t sinceTimestampMs) {
    std::vector<std::string> ids;
    std::unordered_set<std::string> unique;

    try {
        auto filter = stream::document{}
            << "timestamp" << stream::open_document
                << "$gte" << sinceTimestampMs
            << stream::close_document
            << stream::finalize;

        auto projection = stream::document{} << "id" << 1 << stream::finalize;

        mongocxx::options::find opts;
        opts.projection(projection.view());

        auto bidCursor = bids_.find(filter.view(), opts);
        for (auto&& doc : bidCursor) {
            auto j = nlohmann::json::parse(bsoncxx::to_json(doc));
            if (j.contains("id")) unique.insert(j.at("id").get<std::string>());
        }

        auto askCursor = asks_.find(filter.view(), opts);
        for (auto&& doc : askCursor) {
            auto j = nlohmann::json::parse(bsoncxx::to_json(doc));
            if (j.contains("id")) unique.insert(j.at("id").get<std::string>());
        }
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB findOrderIdsSince failed: ") + e.what());
    }

    ids.reserve(unique.size());
    for (auto& id : unique) ids.push_back(std::move(id));
    return ids;
}
