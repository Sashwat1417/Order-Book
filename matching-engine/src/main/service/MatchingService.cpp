#include "MatchingService.hpp"

#include <nlohmann/json.hpp>
#include <iostream>

MatchingService::MatchingService(mongocxx::client& client,
                                 TradeRepository& repo,
                                 TradeProducer& producer,
                                 OrderBook& book)
    : client_(client), repo_(repo), producer_(producer), book_(book)
{}

void MatchingService::processOrder(const std::string& orderJson) {
    std::cout << "[MatchingService] Processing incoming order...\n";

    nlohmann::json j;
    Order order;
    try {
        j     = nlohmann::json::parse(orderJson);
        order = Order::fromJson(j);
    } catch (const std::exception& e) {
        throw MalformedMessageException(
            std::string("Failed to parse order message: ") + e.what()
            + " | raw=" + orderJson);
    }

    std::cout << "[MatchingService] Parsed order"
              << " | id="    << order.id
              << " | side="  << (order.side == Side::BID ? "BID" : "ASK")
              << " | price=" << order.price
              << " | qty="   << order.quantity << "\n";

    auto results = book_.addOrder(order);

    if (results.empty()) {
        std::cout << "[MatchingService] Order " << order.id
                  << " added to book. No match yet.\n";
        return;
    }

    std::cout << "[MatchingService] " << results.size()
              << " trade(s) to persist and publish.\n";

    // One session per processOrder call — Java analogy: one @Transactional method
    // creating a new EntityManager transaction per invocation.
    auto session = client_.start_session();

    for (const auto& result : results) {
        // Wrap the 3 DB writes in a transaction so they all succeed or all roll back.
        // If insertTrade succeeds but either updateOrderStatus fails, MongoDB
        // automatically aborts and none of the writes are visible.
        session.start_transaction();
        try {
            std::cout << "[MatchingService] Persisting trade " << result.trade.id
                      << " to MongoDB...\n";
            repo_.insertTrade(session, result.trade);

            std::cout << "[MatchingService] Updating bid order " << result.bid.id
                      << " → status=" << result.bid.statusToString()
                      << " | remaining=" << result.bid.remaining << "\n";
            repo_.updateOrderStatus(session, result.bid.id, result.bid.statusToString(), result.bid.remaining);

            std::cout << "[MatchingService] Updating ask order " << result.ask.id
                      << " → status=" << result.ask.statusToString()
                      << " | remaining=" << result.ask.remaining << "\n";
            repo_.updateOrderStatus(session, result.ask.id, result.ask.statusToString(), result.ask.remaining);

            session.commit_transaction();
            std::cout << "[MatchingService] Trade " << result.trade.id << " committed to MongoDB.\n";
        } catch (const std::exception& e) {
            session.abort_transaction();
            throw std::runtime_error(
                std::string("Transaction aborted — no partial writes persisted: ") + e.what());
        }

        std::string tradeJson = result.trade.toJson().dump();
        std::cout << "[MatchingService] Publishing trade to Kafka: " << tradeJson << "\n";
        try {
            producer_.publish(tradeJson);
        } catch (const std::exception& e) {
            // DB committed but Kafka publish failed.
            // NOTE: In production this should be handled via an outbox pattern.
            std::cerr << "[MatchingService] CRITICAL: trade " << result.trade.id
                      << " committed to MongoDB but Kafka publish failed — manual recovery required: "
                      << e.what() << "\n";
            throw;
        }

        std::cout << "[MatchingService] Trade " << result.trade.id
                  << " published successfully."
                  << " | price=" << result.trade.price
                  << " | qty="   << result.trade.quantity << "\n";
    }
}
