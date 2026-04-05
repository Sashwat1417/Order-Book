#pragma once

#include <string>
#include <functional>
#include <memory>
#include <atomic>
#include <librdkafka/rdkafkacpp.h>

// Java analogy: like a @KafkaListener with AckMode.MANUAL_IMMEDIATE —
// we only call acknowledgment (commitSync) after the handler succeeds.
// If the handler throws, the offset is NOT committed and Kafka redelivers.
class OrderListener {
public:
    OrderListener(const std::string& brokers,
                  const std::string& topic,
                  const std::string& groupId);
    ~OrderListener();

    // Poll once (500 ms timeout).
    // Commits offset only when handler returns without throwing.
    void poll(const std::function<void(const std::string&)>& handler);

    void stop();
    bool isRunning() const;

private:
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    std::atomic<bool> running_{true};
};
