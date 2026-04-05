#pragma once

#include <string>
#include <cstdint>

#include "Order.hpp"
#include "Trade.hpp"

// Java analogy: a final utility class with static methods —
// think DateTimeUtils or IdGenerator.
namespace MatchingHelper {
    int64_t     currentTimeMs();
    std::string generateTradeId();
    Trade       buildTrade(const Order& bid, const Order& ask, double fillQty);
}
