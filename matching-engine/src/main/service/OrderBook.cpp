#include "OrderBook.hpp"
#include "MatchingHelper.hpp"
#include "dedup/RedisDeduplicator.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>

namespace {
    constexpr double EPSILON = 1e-9;

    bool isZero(double val) {
        return std::abs(val) < EPSILON;
    }

    int64_t nowMs() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }
}

OrderBook::OrderBook(std::unique_ptr<RedisDeduplicator> deduplicator)
    : deduplicator_(std::move(deduplicator)) {}

OrderBook::PriceKey OrderBook::toPriceKey(double price) {
    // Round to nearest cent to avoid splitting equivalent prices across buckets.
    return static_cast<PriceKey>(std::llround(price * static_cast<double>(kPriceScale)));
}

double OrderBook::fromPriceKey(PriceKey key) {
    return static_cast<double>(key) / static_cast<double>(kPriceScale);
}

void OrderBook::rememberId(const std::string& id) {
    const int64_t seenAt = nowMs();
    knownIds_[id] = seenAt;
    knownIdEvictionQueue_.push_back({id, seenAt});
    evictKnownIds(seenAt);
}

bool OrderBook::isKnownId(const std::string& id) const {
    return knownIds_.find(id) != knownIds_.end();
}

bool OrderBook::tryMarkOrderIdSeen(const std::string& id) {
    if (deduplicator_) return deduplicator_->tryMarkSeen(id);
    if (isKnownId(id)) return false;
    rememberId(id);
    return true;
}

void OrderBook::evictKnownIds(int64_t nowMsValue) {
    // Defaults chosen to keep dedupe effective for typical Kafka replay windows
    // while bounding memory in long-running processes.
    static constexpr int64_t kKnownIdTtlMs = 24LL * 60 * 60 * 1000; // 24 hours
    static constexpr size_t kMaxKnownIds = 1'000'000;

    while (!knownIdEvictionQueue_.empty()) {
        const auto& front = knownIdEvictionQueue_.front();
        const std::string& id = front.first;
        const int64_t seenAt = front.second;

        const bool ttlExpired = (nowMsValue - seenAt) > kKnownIdTtlMs;
        const bool overLimit = knownIds_.size() > kMaxKnownIds;
        if (!ttlExpired && !overLimit) break;

        auto it = knownIds_.find(id);
        if (it != knownIds_.end() && it->second == seenAt) {
            knownIds_.erase(it);
        }
        knownIdEvictionQueue_.pop_front();
    }
}

void OrderBook::loadOrder(const Order& order) {
    // Seeding must not be gated by dedupe. We only mark the ID as seen to avoid
    // re-processing Kafka replays for the same order.
    if (deduplicator_) {
        (void)deduplicator_->tryMarkSeen(order.id);
    } else {
        rememberId(order.id);
    }
    if (order.side == Side::BID) {
        bids_[toPriceKey(order.price)].push_back(order);
    } else {
        asks_[toPriceKey(order.price)].push_back(order);
    }
    std::cout << "[OrderBook] Seeded " << (order.side == Side::BID ? "BID" : "ASK")
              << " order | id=" << order.id
              << " | price=" << order.price
              << " | remaining=" << order.remaining
              << " | status=" << order.statusToString() << "\n";
}

std::vector<MatchResult> OrderBook::addOrder(Order order) {
    if (!tryMarkOrderIdSeen(order.id)) {
        std::cout << "[OrderBook] Duplicate order skipped (already processed): " << order.id << "\n";
        return {};
    }

    std::cout << "[OrderBook] New " << (order.side == Side::BID ? "BID" : "ASK")
              << " order added | id=" << order.id
              << " | price=" << order.price
              << " | qty=" << order.quantity << "\n"
              << "[OrderBook] Book state → bids levels=" << bids_.size()
              << " | asks levels=" << asks_.size() << "\n";

    if (order.side == Side::BID) {
        bids_[toPriceKey(order.price)].push_back(order);
    } else {
        asks_[toPriceKey(order.price)].push_back(order);
    }
    return match();
}

std::vector<MatchResult> OrderBook::match() {
    std::vector<MatchResult> results;

    while (!bids_.empty() && !asks_.empty()) {
        PriceKey bestBidPriceKey = bids_.begin()->first;
        PriceKey bestAskPriceKey = asks_.begin()->first;
        double bestBidPrice = fromPriceKey(bestBidPriceKey);
        double bestAskPrice = fromPriceKey(bestAskPriceKey);

        std::cout << "[OrderBook] Checking match → best bid=" << bestBidPrice
                  << " | best ask=" << bestAskPrice << "\n";

        if (bestBidPriceKey < bestAskPriceKey) {
            std::cout << "[OrderBook] No overlap — book resting. Waiting for next order.\n";
            break;
        }

        std::deque<Order>& bidQueue = bids_.begin()->second;
        std::deque<Order>& askQueue = asks_.begin()->second;

        Order& bid = bidQueue.front();
        Order& ask = askQueue.front();

        double fillQty = std::min(bid.remaining, ask.remaining);

        bid.remaining -= fillQty;
        ask.remaining -= fillQty;

        if (isZero(bid.remaining)) bid.remaining = 0.0;
        if (isZero(ask.remaining)) ask.remaining = 0.0;

        bid.status = isZero(bid.remaining) ? OrderStatus::FILLED : OrderStatus::PARTIALLY_FILLED;
        ask.status = isZero(ask.remaining) ? OrderStatus::FILLED : OrderStatus::PARTIALLY_FILLED;

        std::cout << "[OrderBook] MATCH FOUND!"
                  << " | bid=" << bid.id << " (" << bid.statusToString() << ")"
                  << " | ask=" << ask.id << " (" << ask.statusToString() << ")"
                  << " | fill qty=" << fillQty
                  << " | execution price=" << ask.price << "\n";

        results.push_back(MatchResult{
            MatchingHelper::buildTrade(bid, ask, fillQty),
            bid,
            ask
        });

        if (isZero(bid.remaining)) {
            bidQueue.pop_front();
            if (bidQueue.empty()) bids_.erase(bestBidPriceKey);
        }
        if (isZero(ask.remaining)) {
            askQueue.pop_front();
            if (askQueue.empty()) asks_.erase(bestAskPriceKey);
        }
    }

    if (results.empty()) {
        std::cout << "[OrderBook] No trades produced for this order.\n";
    } else {
        std::cout << "[OrderBook] " << results.size() << " trade(s) produced.\n";
    }

    return results;
}
