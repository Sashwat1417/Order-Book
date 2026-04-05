#include "TradeListener.hpp"

#include <iostream>
#include <stdexcept>

TradeListener::TradeListener(const std::string& brokers,
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

    std::cout << "[TradeListener] Connected to brokers : " << brokers  << "\n"
              << "[TradeListener] Consumer group       : " << groupId  << "\n"
              << "[TradeListener] Subscribed to topic  : " << topic_   << "\n"
              << "[TradeListener] Auto-commit          : disabled (manual commit after email sent)\n";
}

TradeListener::~TradeListener() {
    std::cout << "[TradeListener] Closing Kafka consumer.\n";
    consumer_->close();
}

void TradeListener::poll(const std::function<void(const std::string&)>& handler) {
    RdKafka::Message* msg = consumer_->consume(500);
    if (!msg) return;

    switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR: {
            std::string payload(static_cast<const char*>(msg->payload()), msg->len());
            std::cout << "[TradeListener] Trade event received"
                      << " | partition=" << msg->partition()
                      << " | offset="    << msg->offset()
                      << " | size="      << msg->len() << " bytes\n"
                      << "[TradeListener] Payload: " << payload << "\n";
            try {
                handler(payload);
                consumer_->commitSync(msg);
                std::cout << "[TradeListener] Offset " << msg->offset()
                          << " committed — email sent successfully.\n";
            } catch (const std::exception& e) {
                std::cerr << "[TradeListener] Handler failed — offset " << msg->offset()
                          << " NOT committed (will retry): " << e.what() << "\n";
            }
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            break;

        default:
            std::cerr << "[TradeListener] Kafka error: " << msg->errstr() << "\n";
            break;
    }

    delete msg;
}

void TradeListener::stop() {
    std::cout << "[TradeListener] Stop requested.\n";
    running_ = false;
}

bool TradeListener::isRunning() const { return running_; }
