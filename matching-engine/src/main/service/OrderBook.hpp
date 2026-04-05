#pragma once

#include <map>
#include <deque>
#include <vector>
#include <functional>
#include <unordered_set>
#include <string>

#include "Order.hpp"
#include "Trade.hpp"

struct MatchResult {
    Trade trade;
    Order bid;   // bid snapshot after the fill (has updated remaining + status)
    Order ask;   // ask snapshot after the fill
};

// Java analogy: a stateful @Component holding two TreeMaps.
// Bids: TreeMap<Double, Deque<Order>> sorted descending (highest price first).
// Asks: TreeMap<Double, Deque<Order>> sorted ascending  (lowest price first).
class OrderBook {
public:
    // Add an order and run the match loop.
    // Returns every trade produced (zero or more).
    std::vector<MatchResult> addOrder(Order order);

    // Seed an existing order into the book WITHOUT running match.
    // Used on startup to restore state from MongoDB.
    void loadOrder(const Order& order);

private:
    std::map<double, std::deque<Order>, std::greater<double>> bids_;
    std::map<double, std::deque<Order>>                       asks_;

    // Tracks every order ID the book has ever seen (seeded or processed).
    // Used to skip duplicate Kafka messages on replay after restart.
    // Java analogy: like a Set<String> deduplication cache.
    std::unordered_set<std::string> knownIds_;

    std::vector<MatchResult> match();
};
