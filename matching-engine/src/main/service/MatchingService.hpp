#pragma once

#include <stdexcept>
#include <string>

#include <mongocxx/client.hpp>

#include "OrderBook.hpp"
#include "TradeRepository.hpp"
#include "TradeProducer.hpp"

// Thrown for permanently malformed messages (bad JSON, missing fields).
// OrderListener catches this, logs it, and commits the offset to skip the
// poison message rather than retrying forever.
// Java analogy: a custom RuntimeException that a @KafkaListener error handler
// routes to a dead-letter topic instead of retrying.
struct MalformedMessageException : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Java analogy: a @Service that orchestrates three collaborators —
// the in-memory book, the MongoDB repository, and the Kafka producer.
class MatchingService {
public:
    // client is needed to create sessions for multi-document transactions.
    // Java analogy: constructor injection of all four @Autowired dependencies.
    MatchingService(mongocxx::client& client,
                    TradeRepository& repo,
                    TradeProducer& producer,
                    OrderBook& book);

    // Parse the raw Kafka message JSON, run it through the order book,
    // persist every resulting trade atomically, and publish each trade to Kafka.
    void processOrder(const std::string& orderJson);

private:
    mongocxx::client& client_;
    TradeRepository&  repo_;
    TradeProducer&    producer_;
    OrderBook&        book_;   // injected — seeded before first message arrives
};
