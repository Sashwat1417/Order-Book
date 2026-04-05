#include "OutboxRelay.hpp"

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/change_stream.hpp>
#include <mongocxx/exception/exception.hpp>
#include <nlohmann/json.hpp>

#include <iostream>
#include <thread>
#include <chrono>

namespace stream = bsoncxx::builder::stream;

OutboxRelay::OutboxRelay(const std::string& mongoUri,
                         const std::string& dbName,
                         KafkaProducer& producer)
    : mongoUri_(mongoUri)
    , dbName_(dbName)
    , producer_(producer)
{}

void OutboxRelay::stop() {
    running_ = false;
}

bool OutboxRelay::drainPending(mongocxx::collection& outbox) {
    while (running_) {
        // Fetch next batch: PENDING records oldest first, bounded by BATCH_SIZE
        auto filter = stream::document{} << "status" << "PENDING" << stream::finalize;
        auto sort   = stream::document{} << "insertedAt" << 1 << stream::finalize;
        auto proj   = stream::document{} << "_id" << 0 << stream::finalize;

        mongocxx::options::find opts;
        opts.sort(sort.view());
        opts.projection(proj.view());
        opts.limit(BATCH_SIZE);

        // Collect into vector — cursor must be fully consumed before next DB call
        std::vector<std::pair<std::string, std::string>> batch;  // {id, payload}
        try {
            auto cursor = outbox.find(filter.view(), opts);
            for (auto&& doc : cursor) {
                auto j = nlohmann::json::parse(bsoncxx::to_json(doc));
                batch.push_back({ j["id"].get<std::string>(),
                                  j["payload"].get<std::string>() });
            }
        } catch (const mongocxx::exception& e) {
            std::cerr << "[OutboxRelay] MongoDB read failed: " << e.what() << "\n";
            return false;
        }

        if (batch.empty()) {
            std::cout << "[OutboxRelay] No PENDING records — all caught up.\n";
            return true;  // nothing left to publish
        }

        std::cout << "[OutboxRelay] Batch fetched: " << batch.size()
                  << " PENDING record(s) (max batch=" << BATCH_SIZE << ").\n";

        int published = 0;
        for (const auto& [id, payload] : batch) {
            std::cout << "[OutboxRelay] → Publishing to Kafka"
                      << " | id=" << id
                      << " | payload=" << payload << "\n";

            try {
                producer_.publish(payload);
                published++;
            } catch (const std::exception& e) {
                std::cerr << "[OutboxRelay] Kafka publish FAILED for id=" << id
                          << " — stopping batch, will retry in "
                          << RETRY_BACKOFF_MS << "ms: " << e.what() << "\n";
                std::cerr << "[OutboxRelay] Progress: " << published << "/"
                          << batch.size() << " published before failure.\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_BACKOFF_MS));
                return false;  // stop here — retry from this record next time
            }

            // Mark SENT only AFTER successful Kafka publish
            try {
                auto idFilter = stream::document{} << "id" << id << stream::finalize;
                auto update   = stream::document{}
                    << "$set" << stream::open_document
                        << "status" << "SENT"
                    << stream::close_document
                    << stream::finalize;
                outbox.update_one(idFilter.view(), update.view());
                std::cout << "[OutboxRelay] ✓ Marked SENT in outbox | id=" << id << "\n";
            } catch (const mongocxx::exception& e) {
                // Published to Kafka but failed to mark SENT.
                // On restart this record will be published again (at-least-once).
                // matching-engine's knownIds_ handles the duplicate safely.
                std::cerr << "[OutboxRelay] WARNING: published to Kafka but failed"
                          << " to mark SENT | id=" << id
                          << " | duplicate delivery possible on restart: "
                          << e.what() << "\n";
            }
        }

        std::cout << "[OutboxRelay] Batch done: " << published
                  << "/" << batch.size() << " published to Kafka.\n";

        // If batch was full, there may be more PENDING — loop again
        if ((int)batch.size() < BATCH_SIZE) return true;
    }
    return true;
}

void OutboxRelay::run() {
    std::cout << "[OutboxRelay] Started on background thread.\n";

    // Create our own mongocxx::client on THIS thread.
    // Java analogy: like a Runnable that opens its own JDBC connection —
    // each thread owns its connection; the DB server handles concurrent access.
    mongocxx::client client{mongocxx::uri{mongoUri_}};
    auto db     = client[dbName_];
    auto outbox = db["outbox"];

    std::cout << "[OutboxRelay] MongoDB client created on relay thread.\n";

    // Phase 1 — Catch-up: publish any PENDING records left from before startup.
    // Handles crash recovery — records that were written but never published.
    std::cout << "[OutboxRelay] Phase 1: catching up on existing PENDING records...\n";
    drainPending(outbox);
    std::cout << "[OutboxRelay] Phase 1 complete. Opening change stream...\n";

    // Phase 2 — Change stream: wake up instantly on every new outbox insert.
    // max_await_time(500ms) lets the iterator unblock periodically to check
    // running_ for graceful shutdown — same idea as Kafka poll(500ms).
    try {
        mongocxx::options::change_stream opts;
        opts.max_await_time(std::chrono::milliseconds(500));

        auto cs = outbox.watch(opts);

        std::cout << "[OutboxRelay] Phase 2: change stream open."
                  << " Waiting for new orders...\n";

        // Range-based for correctly handles the iterator lifecycle:
        //   - blocks up to max_await_time(500ms) for each event
        //   - exits the inner for loop on timeout → outer while checks running_
        //   - re-enters the for loop to block again
        // Java analogy: like BlockingQueue.poll(500ms) in a loop
        while (running_) {
            for (const auto& event : cs) {
                std::cout << "[OutboxRelay] Change stream event received"
                          << " — new outbox record inserted.\n";
                drainPending(outbox);
                if (!running_) break;
            }
            // for loop exits on 500ms timeout → check running_ → re-enter
        }
    } catch (const mongocxx::exception& e) {
        if (running_) {
            std::cerr << "[OutboxRelay] Change stream error: " << e.what()
                      << " — relay stopped.\n";
        }
    }

    std::cout << "[OutboxRelay] Stopped.\n";
}
