#pragma once

#include <string>
#include <memory>
#include <librdkafka/rdkafkacpp.h>

// Java analogy: like Spring's KafkaTemplate<String, String> scoped to the trades topic.
class TradeProducer {
public:
    TradeProducer(const std::string& brokers, const std::string& topic);
    void publish(const std::string& message);

private:
    std::unique_ptr<RdKafka::Producer> producer_;
    std::string topic_;
};
