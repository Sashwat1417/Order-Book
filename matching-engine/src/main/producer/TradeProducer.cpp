#include "TradeProducer.hpp"
#include <stdexcept>

TradeProducer::TradeProducer(const std::string& brokers, const std::string& topic)
    : topic_(topic)
{
    std::string errstr;
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);

    producer_.reset(RdKafka::Producer::create(conf, errstr));
    delete conf;

    if (!producer_) {
        throw std::runtime_error("Failed to create Kafka producer: " + errstr);
    }
}

void TradeProducer::publish(const std::string& message) {
    RdKafka::ErrorCode err = producer_->produce(
        topic_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(message.c_str()),
        message.size(),
        nullptr, 0, 0, nullptr
    );
    producer_->poll(0);

    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error(
            std::string("[TradeProducer] Produce failed: ") + RdKafka::err2str(err));
    }
}
