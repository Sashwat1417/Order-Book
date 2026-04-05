#pragma once

#include <map>
#include <deque>
#include <vector>
#include <functional>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <utility>
#include <memory>

class RedisDeduplicator;

#include "Order.hpp"
#include "Trade.hpp"

struct MatchResult {
    Trade trade;
    Order bid;   // bid snapshot after the fill (has updated remaining + status)
    Order ask;   // ask snapshot after the fill
};

// Java analogy: a stateful @Component holding two TreeMaps.
// Bids: TreeMap<Long, Deque<Order>> sorted descending (highest price first).
// Asks: TreeMap<Long, Deque<Order>> sorted ascending  (lowest price first).
class OrderBook {
public:
    explicit OrderBook(std::unique_ptr<RedisDeduplicator> deduplicator = nullptr);

    // Add an order and run the match loop.
    // Returns every trade produced (zero or more).
    std::vector<MatchResult> addOrder(Order order);

    // Seed an existing order into the book WITHOUT running match.
    // Used on startup to restore state from MongoDB.
    void loadOrder(const Order& order);

    // Run one match sweep against the current book state.
    // Called once after all open orders are seeded to resolve any crossings
    // that existed before a crash (e.g. partially-filled bid vs open ask).
    std::vector<MatchResult> runPostSeedMatch();

private:
    // Use integer price keys to avoid floating-point map bucketing issues.
    // NOTE: This assumes prices are expressed with 2 decimal places (cents).
    using PriceKey = int64_t;
    static constexpr PriceKey kPriceScale = 100;

    std::map<PriceKey, std::deque<Order>, std::greater<PriceKey>> bids_;
    std::map<PriceKey, std::deque<Order>>                         asks_;

    // Tracks order IDs to dedupe Kafka replays. Bounded via TTL + max-size eviction.
    std::unordered_map<std::string, int64_t> knownIds_; // id -> seenAtMs
    std::deque<std::pair<std::string, int64_t>> knownIdEvictionQueue_;

    std::unique_ptr<RedisDeduplicator> deduplicator_;

    std::vector<MatchResult> match();

    static PriceKey toPriceKey(double price);
    static double fromPriceKey(PriceKey key);

    void rememberId(const std::string& id);
    bool isKnownId(const std::string& id) const;
    void evictKnownIds(int64_t nowMs);

    bool tryMarkOrderIdSeen(const std::string& id);
};
