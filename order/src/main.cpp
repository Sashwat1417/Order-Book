#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <httplib.h>
#include <iostream>
#include <cstdlib>
#include <thread>
#include <csignal>

#include "db/OrderRepository.hpp"
#include "kafka/KafkaProducer.hpp"
#include "service/OrderService.hpp"
#include "api/OrderController.hpp"
#include "outbox/OutboxRelay.hpp"

static std::string env(const char* key, const char* defaultVal) {
    const char* val = std::getenv(key);
    return val ? std::string(val) : std::string(defaultVal);
}

static OutboxRelay* g_relay = nullptr;

static void signalHandler(int) {
    if (g_relay) g_relay->stop();
}

int main() {
    signal(SIGINT,  signalHandler);
    signal(SIGTERM, signalHandler);

    const std::string mongoUri     = env("MONGO_URI",     "mongodb://localhost:27017");
    const std::string kafkaBrokers = env("KAFKA_BROKERS", "localhost:9092");
    const std::string kafkaTopic   = env("KAFKA_TOPIC",   "new-orders");
    const int         serverPort   = std::stoi(env("SERVER_PORT", "3000"));

    std::cout << "[API] Config:\n"
              << "  MONGO_URI:     " << mongoUri     << "\n"
              << "  KAFKA_BROKERS: " << kafkaBrokers << "\n"
              << "  KAFKA_TOPIC:   " << kafkaTopic   << "\n"
              << "  SERVER_PORT:   " << serverPort   << "\n";

    mongocxx::instance instance{};
    mongocxx::client   mongoClient{mongocxx::uri{mongoUri}};
    auto db = mongoClient["orderbook"];

    // V2: pass client to repository (needed for transactions)
    OrderRepository repository(mongoClient, db);

    // KafkaProducer is now used only by the OutboxRelay, not by OrderService
    KafkaProducer kafkaProducer(kafkaBrokers, kafkaTopic);

    // OutboxRelay — runs on its own background thread.
    // Passes URI + dbName strings so the relay can create its own mongocxx::client
    // on the background thread (mongocxx objects are not thread-safe across threads).
    // Java analogy: like a @Scheduled single-threaded executor watching the outbox,
    // where the executor opens its own JDBC connection rather than sharing one.
    OutboxRelay relay(mongoUri, "orderbook", kafkaProducer);
    g_relay = &relay;

    // Start relay on a dedicated background thread
    // Java analogy: Executors.newSingleThreadExecutor().submit(() -> relay.run())
    std::thread relayThread([&relay]() {
        relay.run();
    });

    // Wire HTTP layer
    OrderService    service(repository);
    OrderController controller(service);

    httplib::Server server;
    controller.registerRoutes(server);

    std::cout << "[API] Server starting on port " << serverPort << "\n";
    server.listen("0.0.0.0", serverPort);  // blocks until server.stop()

    // Graceful shutdown
    relay.stop();
    relayThread.join();
    std::cout << "[API] Shutdown complete.\n";

    return 0;
}
