#include <iostream>
#include <csignal>
#include <atomic>
#include <cstdlib>
#include <stdexcept>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include "listener/OrderListener.hpp"
#include "producer/TradeProducer.hpp"
#include "db/TradeRepository.hpp"
#include "service/MatchingService.hpp"

// Java analogy: static volatile boolean running — used for graceful shutdown
// via SIGINT / SIGTERM (Ctrl-C or kill).
static std::atomic<bool> g_running{true};
static void signalHandler(int) { g_running = false; }

int main() {
    signal(SIGINT,  signalHandler);
    signal(SIGTERM, signalHandler);

    // Read config from environment variables (12-factor style)
    auto getenv_or = [](const char* key, const char* def) -> std::string {
        const char* val = std::getenv(key);
        return val ? val : def;
    };

    std::string mongoUri    = getenv_or("MONGO_URI",           "mongodb://localhost:27017");
    std::string brokers     = getenv_or("KAFKA_BROKERS",       "localhost:9092");
    std::string inputTopic  = getenv_or("KAFKA_INPUT_TOPIC",   "new-orders");
    std::string outputTopic = getenv_or("KAFKA_OUTPUT_TOPIC",  "trades");
    std::string groupId     = getenv_or("KAFKA_GROUP_ID",      "matching-engine-group");

    try {
        // MongoDB setup — one instance per process (mongocxx requirement)
        mongocxx::instance instance{};
        mongocxx::client   client{mongocxx::uri{mongoUri}};
        auto db = client["orderbook"];

        TradeRepository repo(db);
        TradeProducer   producer(brokers, outputTopic);

        // ── Startup seeding ───────────────────────────────────────────────
        // Load all OPEN/PARTIALLY_FILLED orders from MongoDB into the book
        // BEFORE starting the Kafka consumer. This restores in-memory state
        // after a restart without replaying old Kafka messages.
        //
        // Java analogy: like an @PostConstruct method that queries the DB
        // and rebuilds an in-memory cache before the app starts serving.
        //
        // NOTE: There is a small window between the DB query and Kafka starting
        // where new orders could arrive. In production this is closed by
        // recording the Kafka offset BEFORE the DB query and starting from
        // that offset — acceptable gap for this project.
        // ─────────────────────────────────────────────────────────────────
        OrderBook book;
        auto openOrders = repo.findOpenOrders();
        for (const auto& order : openOrders) {
            book.loadOrder(order);   // insert without matching — already processed
        }
        std::cout << "[MatchingEngine] Seeded " << openOrders.size()
                  << " open order(s) from MongoDB.\n";

        MatchingService service(repo, producer, book);
        OrderListener   listener(brokers, inputTopic, groupId);

        std::cout << "[MatchingEngine] Started."
                  << " Consuming: " << inputTopic
                  << " → Publishing: " << outputTopic << "\n";

        while (g_running) {
            // Listener calls service.processOrder().
            // On exception: offset is NOT committed — Kafka redelivers the message.
            listener.poll([&](const std::string& msg) {
                service.processOrder(msg);
            });
        }

        std::cout << "[MatchingEngine] Shutting down gracefully.\n";
    } catch (const std::exception& e) {
        std::cerr << "[MatchingEngine] Fatal error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
