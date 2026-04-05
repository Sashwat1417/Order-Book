#include "OrderBook.hpp"
#include "MatchingHelper.hpp"

#include <algorithm>
#include <iostream>

void OrderBook::loadOrder(const Order& order) {
    knownIds_.insert(order.id);
    if (order.side == Side::BID) {
        bids_[order.price].push_back(order);
    } else {
        asks_[order.price].push_back(order);
    }
    std::cout << "[OrderBook] Seeded " << (order.side == Side::BID ? "BID" : "ASK")
              << " order | id=" << order.id
              << " | price=" << order.price
              << " | remaining=" << order.remaining
              << " | status=" << order.statusToString() << "\n";
}

std::vector<MatchResult> OrderBook::addOrder(Order order) {
    if (knownIds_.count(order.id)) {
        std::cout << "[OrderBook] Duplicate order skipped (already processed): " << order.id << "\n";
        return {};
    }
    knownIds_.insert(order.id);

    std::cout << "[OrderBook] New " << (order.side == Side::BID ? "BID" : "ASK")
              << " order added | id=" << order.id
              << " | price=" << order.price
              << " | qty=" << order.quantity << "\n"
              << "[OrderBook] Book state → bids levels=" << bids_.size()
              << " | asks levels=" << asks_.size() << "\n";

    if (order.side == Side::BID) {
        bids_[order.price].push_back(order);
    } else {
        asks_[order.price].push_back(order);
    }
    return match();
}

std::vector<MatchResult> OrderBook::match() {
    std::vector<MatchResult> results;

    while (!bids_.empty() && !asks_.empty()) {
        double bestBidPrice = bids_.begin()->first;
        double bestAskPrice = asks_.begin()->first;

        std::cout << "[OrderBook] Checking match → best bid=" << bestBidPrice
                  << " | best ask=" << bestAskPrice << "\n";

        if (bestBidPrice < bestAskPrice) {
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

        bid.status = (bid.remaining == 0.0) ? OrderStatus::FILLED : OrderStatus::PARTIALLY_FILLED;
        ask.status = (ask.remaining == 0.0) ? OrderStatus::FILLED : OrderStatus::PARTIALLY_FILLED;

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

        if (bid.remaining == 0.0) {
            bidQueue.pop_front();
            if (bidQueue.empty()) bids_.erase(bestBidPrice);
        }
        if (ask.remaining == 0.0) {
            askQueue.pop_front();
            if (askQueue.empty()) asks_.erase(bestAskPrice);
        }
    }

    if (results.empty()) {
        std::cout << "[OrderBook] No trades produced for this order.\n";
    } else {
        std::cout << "[OrderBook] " << results.size() << " trade(s) produced.\n";
    }

    return results;
}
