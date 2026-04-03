#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <httplib.h>
#include <iostream>
#include <cstdlib>

#include "db/OrderRepository.hpp"
#include "kafka/KafkaProducer.hpp"
#include "service/OrderService.hpp"
#include "api/OrderController.hpp"

// Reads an env var — falls back to defaultVal if not set
// Java analogy: @Value("${MONGO_URI:mongodb://localhost:27017}")
static std::string env(const char* key, const char* defaultVal) {
    const char* val = std::getenv(key);
    return val ? std::string(val) : std::string(defaultVal);
}

// Java analogy: this is your @SpringBootApplication main() method
// It wires up all the beans manually (no DI container in C++)
int main() {
    const std::string mongoUri      = env("MONGO_URI",      "mongodb://localhost:27017");
    const std::string kafkaBrokers  = env("KAFKA_BROKERS",  "localhost:9092");
    const std::string kafkaTopic    = env("KAFKA_TOPIC",    "new-orders");
    const int         serverPort    = std::stoi(env("SERVER_PORT", "3000"));

    std::cout << "[API] Config:\n"
              << "  MONGO_URI:     " << mongoUri     << "\n"
              << "  KAFKA_BROKERS: " << kafkaBrokers << "\n"
              << "  KAFKA_TOPIC:   " << kafkaTopic   << "\n"
              << "  SERVER_PORT:   " << serverPort   << "\n";

    // MongoDB — one instance per process, like Spring's ApplicationContext
    mongocxx::instance instance{};
    mongocxx::client   mongoClient{mongocxx::uri{mongoUri}};
    auto db = mongoClient["orderbook"];

    // Wire up layers bottom → top (like Spring's bean initialization order)
    OrderRepository repository(db);
    KafkaProducer   kafkaProducer(kafkaBrokers, kafkaTopic);
    OrderService    service(repository, kafkaProducer);
    OrderController controller(service);

    // HTTP server
    httplib::Server server;
    controller.registerRoutes(server);

    std::cout << "[API] Server starting on port " << serverPort << "\n";
    server.listen("0.0.0.0", serverPort);

    return 0;
}
