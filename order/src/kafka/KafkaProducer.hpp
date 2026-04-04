#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <memory>

// Java analogy: equivalent to Spring's KafkaTemplate<String, String>
class KafkaProducer {
public:
    KafkaProducer(const std::string& brokers, const std::string& topic);
    bool publish(const std::string& message);

private:
    std::unique_ptr<RdKafka::Producer> producer_;
    std::string topic_;
};
