#include "OrderService.hpp"

#include <Order.hpp>
#include <chrono>
#include <random>
#include <stdexcept>
#include <iostream>

using json = nlohmann::json;

static int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

static std::string generateId() {
    thread_local std::mt19937 gen(std::random_device{}());
    thread_local std::uniform_int_distribution<uint32_t> dis;
    return std::to_string(nowMs()) + "-" + std::to_string(dis(gen));
}

OrderService::OrderService(OrderRepository& repository)
    : repository_(repository)
{}

json OrderService::placeOrder(const json& body) {
    if (!body.contains("side") || !body.contains("price") || !body.contains("quantity")) {
        throw std::invalid_argument("Missing required fields: side, price, quantity");
    }

    std::string side = body["side"].get<std::string>();
    if (side != "BID" && side != "ASK") {
        throw std::invalid_argument("side must be BID or ASK");
    }

    double price    = body["price"].get<double>();
    double quantity = body["quantity"].get<double>();

    if (price <= 0 || quantity <= 0) {
        throw std::invalid_argument("price and quantity must be positive");
    }

    Order order;
    order.id        = generateId();
    order.side      = (side == "BID") ? Side::BID : Side::ASK;
    order.price     = price;
    order.quantity  = quantity;
    order.remaining = quantity;
    order.status    = OrderStatus::OPEN;
    order.timestamp = nowMs();

    // V2: atomic MongoDB transaction — inserts order + outbox(PENDING) together.
    // The OutboxRelay background thread picks up the outbox record and
    // publishes to Kafka. No Kafka call here.
    repository_.insertOrderWithOutbox(order);

    std::cout << "[OrderService] Order placed and outbox written"
              << " | id=" << order.id
              << " | " << side << " " << quantity << "@" << price << "\n";

    return order.toJson();
}

json OrderService::getOrderBook() {
    return {
        {"bids", repository_.findOpenBids()},
        {"asks", repository_.findOpenAsks()}
    };
}

bool OrderService::cancelOrder(const std::string& id) {
    bool cancelled = repository_.cancelOrder(id);
    if (cancelled) {
        std::cout << "[OrderService] Order cancelled: " << id << "\n";
    }
    return cancelled;
}
