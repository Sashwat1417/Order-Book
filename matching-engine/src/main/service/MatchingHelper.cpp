#include "MatchingHelper.hpp"

#include <chrono>
#include <random>

namespace MatchingHelper {

int64_t currentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string generateTradeId() {
    thread_local std::mt19937 rng{std::random_device{}()};
    thread_local std::uniform_int_distribution<uint32_t> dist;
    return "trade-" + std::to_string(currentTimeMs())
                    + "-" + std::to_string(dist(rng));
}

// Trade executes at the passive (resting) price — the ask price,
// because the ask was sitting in the book before the bid arrived.
Trade buildTrade(const Order& bid, const Order& ask, double fillQty) {
    return Trade{
        generateTradeId(),
        bid.id,
        ask.id,
        ask.price,   // execution price
        fillQty,
        currentTimeMs()
    };
}

}  // namespace MatchingHelper
