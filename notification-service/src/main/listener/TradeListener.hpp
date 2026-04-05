#pragma once

#include <string>
#include <functional>
#include <memory>
#include <atomic>
#include <librdkafka/rdkafkacpp.h>

// Same manual-commit pattern as OrderListener —
// offset is committed only when the handler succeeds.
// On failure the message is redelivered so the notification is retried.
class TradeListener {
public:
    TradeListener(const std::string& brokers,
                  const std::string& topic,
                  const std::string& groupId);
    ~TradeListener();

    void poll(const std::function<void(const std::string&)>& handler);

    void stop();
    bool isRunning() const;

private:
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    std::atomic<bool> running_{true};
};
