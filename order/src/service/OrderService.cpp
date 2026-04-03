#include "OrderService.hpp"

#include <Order.hpp>
#include <chrono>
#include <random>
#include <stdexcept>
#include <iostream>

using json = nlohmann::json;

// ─── Private Helpers ──────────────────────────────────────────────────────────

static int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

    static std::string generateId() {
    static std::mt19937 gen(std::random_device{}());
    static std::uniform_int_distribution<uint32_t> dis;
    return std::to_string(nowMs()) + "-" + std::to_string(dis(gen));
}

// ─── OrderService ─────────────────────────────────────────────────────────────

OrderService::OrderService(OrderRepository& repository, KafkaProducer& kafkaProducer)
    : repository_(repository)
    , kafkaProducer_(kafkaProducer)
{}

json OrderService::placeOrder(const json& body) {
    // Validate — like a Spring @Valid check
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

    // Build the order
    Order order;
    order.id        = generateId();
    order.side      = (side == "BID") ? Side::BID : Side::ASK;
    order.price     = price;
    order.quantity  = quantity;
    order.remaining = quantity;
    order.status    = OrderStatus::OPEN;
    order.timestamp = nowMs();

    // Persist to MongoDB — if this throws, nothing was written, safe to propagate
    try {
        repository_.insertOrder(order);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("DB insert failed: ") + e.what());
    }

    // Publish to Kafka — if this fails, order is in DB but won't be matched
    // We throw so the caller knows the order is in an inconsistent state
    if (!kafkaProducer_.publish(order.toJson().dump())) {
        throw std::runtime_error("Order saved but failed to publish to Kafka. Order ID: " + order.id);
    }

    std::cout << "[Service] Order placed: " << order.id
              << " " << side << " " << quantity << "@" << price << "\n";

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
        std::cout << "[Service] Order cancelled: " << id << "\n";
    }
    return cancelled;
}
