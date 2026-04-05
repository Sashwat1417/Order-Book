#pragma once

#include <string>

#include "OrderBook.hpp"
#include "TradeRepository.hpp"
#include "TradeProducer.hpp"

// Java analogy: a @Service that orchestrates three collaborators —
// the in-memory book, the MongoDB repository, and the Kafka producer.
class MatchingService {
public:
    // OrderBook is created and seeded externally (in main) then injected here.
    // Java analogy: constructor injection of all three @Autowired dependencies.
    MatchingService(TradeRepository& repo, TradeProducer& producer, OrderBook& book);

    // Parse the raw Kafka message JSON, run it through the order book,
    // persist every resulting trade, and publish each trade to Kafka.
    void processOrder(const std::string& orderJson);

private:
    TradeRepository& repo_;
    TradeProducer&   producer_;
    OrderBook&       book_;   // injected — seeded before first message arrives
};
