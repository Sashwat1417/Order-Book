#include "OrderListener.hpp"
#include "../service/MatchingService.hpp"

#include <iostream>
#include <stdexcept>

OrderListener::OrderListener(const std::string& brokers,
                             const std::string& topic,
                             const std::string& groupId)
    : topic_(topic)
{
    std::string errstr;
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    auto setConf = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error("Kafka config error [" + key + "]: " + errstr);
        }
    };

    setConf("bootstrap.servers", brokers);
    setConf("group.id",          groupId);
    setConf("enable.auto.commit","false");
    setConf("auto.offset.reset", "earliest");

    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;

    if (!consumer_) {
        throw std::runtime_error("Failed to create Kafka consumer: " + errstr);
    }
    consumer_->subscribe({topic_});

    std::cout << "[OrderListener] Connected to brokers : " << brokers      << "\n"
              << "[OrderListener] Consumer group       : " << groupId      << "\n"
              << "[OrderListener] Subscribed to topic  : " << topic_       << "\n"
              << "[OrderListener] Auto-commit          : disabled (manual commit after success)\n";
}

OrderListener::~OrderListener() {
    std::cout << "[OrderListener] Closing Kafka consumer.\n";
    consumer_->close();
}

void OrderListener::poll(const std::function<void(const std::string&)>& handler) {
    RdKafka::Message* msg = consumer_->consume(500);
    if (!msg) return;

    switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR: {
            if (!msg->payload() || msg->len() == 0) {
                std::cerr << "[OrderListener] Empty payload, skipping.\n";
                consumer_->commitSync(msg);
                break;
            }
            std::string payload(static_cast<const char*>(msg->payload()), msg->len());
            std::cout << "[OrderListener] Message received"
                      << " | partition=" << msg->partition()
                      << " | offset="    << msg->offset()
                      << " | size="      << msg->len() << " bytes\n"
                      << "[OrderListener] Payload: " << payload << "\n";
            try {
                handler(payload);
                consumer_->commitSync(msg);
                std::cout << "[OrderListener] Offset " << msg->offset()
                          << " committed successfully.\n";
            } catch (const MalformedMessageException& e) {
                std::cerr << "[OrderListener] Malformed message — offset " << msg->offset()
                          << " skipped (poison message): " << e.what() << "\n";
                consumer_->commitSync(msg);  // advance past it; will never succeed on retry
            } catch (const std::exception& e) {
                std::cerr << "[OrderListener] Handler failed — offset " << msg->offset()
                          << " NOT committed (will retry): " << e.what() << "\n";
            }
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            break;  // normal — no message right now

        default:
            std::cerr << "[OrderListener] Kafka error: " << msg->errstr() << "\n";
            break;
    }

    delete msg;
}

void OrderListener::stop() {
    std::cout << "[OrderListener] Stop requested.\n";
    running_ = false;
}

bool OrderListener::isRunning() const { return running_; }
