#include "MatchingService.hpp"

#include <nlohmann/json.hpp>
#include <iostream>

MatchingService::MatchingService(TradeRepository& repo, TradeProducer& producer, OrderBook& book)
    : repo_(repo), producer_(producer), book_(book)
{}

void MatchingService::processOrder(const std::string& orderJson) {
    std::cout << "[MatchingService] Processing incoming order...\n";

    auto j     = nlohmann::json::parse(orderJson);
    Order order = Order::fromJson(j);

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

    for (const auto& result : results) {
        std::cout << "[MatchingService] Persisting trade " << result.trade.id
                  << " to MongoDB...\n";
        repo_.insertTrade(result.trade);
        std::cout << "[MatchingService] Trade " << result.trade.id << " saved.\n";

        std::cout << "[MatchingService] Updating bid order " << result.bid.id
                  << " → status=" << result.bid.statusToString()
                  << " | remaining=" << result.bid.remaining << "\n";
        repo_.updateOrderStatus(result.bid.id, result.bid.statusToString(), result.bid.remaining);

        std::cout << "[MatchingService] Updating ask order " << result.ask.id
                  << " → status=" << result.ask.statusToString()
                  << " | remaining=" << result.ask.remaining << "\n";
        repo_.updateOrderStatus(result.ask.id, result.ask.statusToString(), result.ask.remaining);

        std::string tradeJson = result.trade.toJson().dump();
        std::cout << "[MatchingService] Publishing trade to Kafka: " << tradeJson << "\n";
        producer_.publish(tradeJson);

        std::cout << "[MatchingService] Trade " << result.trade.id
                  << " published successfully."
                  << " | price=" << result.trade.price
                  << " | qty="   << result.trade.quantity << "\n";
    }
}
