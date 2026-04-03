#include "OrderRepository.hpp"

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/exception/exception.hpp>
#include <stdexcept>

namespace stream = bsoncxx::builder::stream;
using json = nlohmann::json;

OrderRepository::OrderRepository(mongocxx::database& db)
    : bids_(db["bids"])
    , asks_(db["asks"])
{}

void OrderRepository::insertOrder(const Order& order) {
    auto doc = stream::document{}
        << "id"        << order.id
        << "price"     << order.price
        << "quantity"  << order.quantity
        << "remaining" << order.remaining
        << "status"    << order.statusToString()
        << "timestamp" << order.timestamp
        << stream::finalize;

    try {
        auto& collection = (order.side == Side::BID) ? bids_ : asks_;
        auto result = collection.insert_one(doc.view());

        // insert_one returns nullopt if write concern is unacknowledged
        if (!result) {
            throw std::runtime_error("Insert was not acknowledged by MongoDB");
        }
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB insert failed: ") + e.what());
    }
}

// Helper: converts a MongoDB cursor into a JSON array
static json cursorToJson(mongocxx::cursor& cursor) {
    json result = json::array();
    try {
        for (auto&& doc : cursor) {
            result.push_back(json::parse(bsoncxx::to_json(doc)));
        }
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB cursor error: ") + e.what());
    }
    return result;
}

// Helper: filter for orders that are still active
static bsoncxx::document::value openStatusFilter() {
    return stream::document{}
        << "status" << stream::open_document
            << "$in" << stream::open_array
                << "OPEN" << "PARTIALLY_FILLED"
            << stream::close_array
        << stream::close_document
        << stream::finalize;
}

// Projection to exclude MongoDB's internal _id from results
static bsoncxx::document::value excludeId() {
    return stream::document{} << "_id" << 0 << stream::finalize;
}

json OrderRepository::findOpenBids() {
    try {
        mongocxx::options::find opts;
        opts.sort(stream::document{} << "price" << -1 << stream::finalize);
        opts.projection(excludeId().view());
        auto filter = openStatusFilter();
        auto cursor = bids_.find(filter.view(), opts);
        return cursorToJson(cursor);
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB find bids failed: ") + e.what());
    }
}

json OrderRepository::findOpenAsks() {
    try {
        mongocxx::options::find opts;
        opts.sort(stream::document{} << "price" << 1 << stream::finalize);
        opts.projection(excludeId().view());
        auto filter = openStatusFilter();
        auto cursor = asks_.find(filter.view(), opts);
        return cursorToJson(cursor);
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB find asks failed: ") + e.what());
    }
}

bool OrderRepository::cancelOrder(const std::string& id) {
    try {
        auto filter = stream::document{} << "id" << id << stream::finalize;
        auto update = stream::document{}
            << "$set" << stream::open_document
                << "status" << "CANCELLED"
            << stream::close_document
            << stream::finalize;

        auto bidResult = bids_.update_one(filter.view(), update.view());
        auto askResult = asks_.update_one(filter.view(), update.view());

        return (bidResult && bidResult->modified_count() > 0) ||
               (askResult && askResult->modified_count() > 0);

    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB cancel failed: ") + e.what());
    }
}
