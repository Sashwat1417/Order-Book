#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <atomic>
#include <string>

#include "../kafka/KafkaProducer.hpp"

// Java analogy: a @Component with a @Scheduled method running on a
// single dedicated thread — reads PENDING outbox records and publishes
// them to Kafka in strict insertedAt order.
//
// Uses MongoDB change streams to wake up instantly on new inserts
// instead of sleeping on a fixed poll interval.
// On startup (or restart) it first drains all existing PENDING records
// before opening the change stream — guarantees no records are missed.
//
// Thread-safety note: mongocxx objects (client, collection) must be created
// and used on the same thread. OutboxRelay creates its own mongocxx::client
// inside run() on the background thread — same pattern as a JDBC connection
// per thread in a Java thread pool.
class OutboxRelay {
public:
    // Takes URI + dbName strings so the background thread can create its
    // own mongocxx::client (mongocxx objects are not thread-safe across threads).
    OutboxRelay(const std::string& mongoUri,
                const std::string& dbName,
                KafkaProducer& producer);

    // Blocking run loop — call this on a dedicated background thread.
    // Creates its own mongocxx::client on entry (thread-local ownership).
    // Returns only when stop() is called.
    void run();

    void stop();

private:
    std::string       mongoUri_;
    std::string       dbName_;
    KafkaProducer&    producer_;
    std::atomic<bool> running_{true};

    static constexpr int BATCH_SIZE       = 100;
    static constexpr int RETRY_BACKOFF_MS = 200;

    // Publishes all PENDING records in batches of BATCH_SIZE,
    // ordered by insertedAt ASC.
    // Returns true if all were published, false if a Kafka failure occurred.
    bool drainPending(mongocxx::collection& outbox);
};
