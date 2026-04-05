#pragma once

#include <string>
#include "EmailHelper.hpp"

// Java analogy: a @Service that receives trade events and sends email notifications.
class NotificationService {
public:
    explicit NotificationService(const EmailHelper::SmtpConfig& smtpConfig);

    // Parse the raw Kafka message JSON and send a formatted email for the trade.
    void processTradeEvent(const std::string& tradeJson);

private:
    EmailHelper::SmtpConfig smtpConfig_;
};
