#include "OrderRepository.hpp"

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/exception/exception.hpp>
#include <chrono>
#include <stdexcept>

namespace stream = bsoncxx::builder::stream;
using json = nlohmann::json;

OrderRepository::OrderRepository(mongocxx::client& client, mongocxx::database& db)
    : client_(client)
    , bids_(db["bids"])
    , asks_(db["asks"])
    , outbox_(db["outbox"])
{}

void OrderRepository::insertOrderWithOutbox(const Order& order) {
    // Start a MongoDB client session for multi-document transaction support.
    // Java analogy: like @Transactional — both inserts succeed or both roll back.
    auto session = client_.start_session();

    try {
        session.start_transaction();

        // 1. Insert the order into bids or asks collection
        auto orderDoc = stream::document{}
            << "id"        << order.id
            << "side"      << (order.side == Side::BID ? "BID" : "ASK")
            << "price"     << order.price
            << "quantity"  << order.quantity
            << "remaining" << order.remaining
            << "status"    << order.statusToString()
            << "timestamp" << order.timestamp
            << stream::finalize;

        auto& collection = (order.side == Side::BID) ? bids_ : asks_;
        collection.insert_one(session, orderDoc.view());

        // 2. Insert outbox record in the same transaction.
        // The relay reads status=PENDING, publishes to Kafka, then marks SENT.
        int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        auto outboxDoc = stream::document{}
            << "id"         << order.id
            << "payload"    << order.toJson().dump()
            << "status"     << "PENDING"
            << "insertedAt" << nowMs
            << stream::finalize;

        outbox_.insert_one(session, outboxDoc.view());

        session.commit_transaction();

        std::cout << "[OrderRepository] Order + outbox written atomically"
                  << " | id=" << order.id << "\n";

    } catch (const mongocxx::exception& e) {
        session.abort_transaction();
        throw std::runtime_error(
            std::string("Transaction failed, rolled back: ") + e.what());
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
        // Store all bsoncxx documents in named variables — views must not outlive their owner
        auto sort_doc       = stream::document{} << "price" << -1 << stream::finalize;
        auto projection_doc = excludeId();
        auto filter         = openStatusFilter();

        mongocxx::options::find opts;
        opts.sort(sort_doc.view());
        opts.projection(projection_doc.view());

        auto cursor = bids_.find(filter.view(), opts);
        return cursorToJson(cursor);
    } catch (const mongocxx::exception& e) {
        throw std::runtime_error(std::string("MongoDB find bids failed: ") + e.what());
    }
}

json OrderRepository::findOpenAsks() {
    try {
        auto sort_doc       = stream::document{} << "price" << 1 << stream::finalize;
        auto projection_doc = excludeId();
        auto filter         = openStatusFilter();

        mongocxx::options::find opts;
        opts.sort(sort_doc.view());
        opts.projection(projection_doc.view());

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
