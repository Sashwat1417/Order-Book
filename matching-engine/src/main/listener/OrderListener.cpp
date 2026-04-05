#include "OrderListener.hpp"

#include <iostream>
#include <stdexcept>

OrderListener::OrderListener(const std::string& brokers,
                             const std::string& topic,
                             const std::string& groupId)
    : topic_(topic)
{
    std::string errstr;
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id",          groupId, errstr);
    conf->set("enable.auto.commit","false",  errstr);
    conf->set("auto.offset.reset", "earliest", errstr);

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
