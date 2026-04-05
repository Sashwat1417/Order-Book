#include "NotificationService.hpp"

#include <nlohmann/json.hpp>
#include <sstream>
#include <ctime>
#include <iostream>

#include "Trade.hpp"

NotificationService::NotificationService(const EmailHelper::SmtpConfig& smtpConfig)
    : smtpConfig_(smtpConfig)
{}

void NotificationService::processTradeEvent(const std::string& tradeJson) {
    std::cout << "[NotificationService] Processing trade event...\n";

    nlohmann::json j;
    try {
        j = nlohmann::json::parse(tradeJson);
    } catch (const nlohmann::json::parse_error& e) {
        std::cerr << "[NotificationService] Failed to parse trade JSON: " << e.what() << "\n";
        return; // or throw a domain-specific exception to signal poison message
    }
    Trade trade = Trade::fromJson(j);

    std::cout << "[NotificationService] Trade parsed"
              << " | id="    << trade.id
              << " | price=" << trade.price
              << " | qty="   << trade.quantity
              << " | bid="   << trade.bidOrderId
              << " | ask="   << trade.askOrderId << "\n";

    // Format timestamp (Unix ms → readable UTC string)
    std::time_t sec = static_cast<std::time_t>(trade.timestamp / 1000);
    std::tm tm_utc{};
#ifdef _WIN32
    gmtime_s(&tm_utc, &sec);
#else
    gmtime_r(&sec, &tm_utc);
#endif
    char timeBuf[32];
    std::strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);

    std::string subject = "Trade Executed — " + trade.id;

    std::ostringstream body;
    body << "A trade has been executed on your order book.\r\n\r\n";
    body << "Trade Details\r\n";
    body << "=============\r\n";
    body << "Trade ID   : " << trade.id        << "\r\n";
    body << "Price      : " << trade.price      << "\r\n";
    body << "Quantity   : " << trade.quantity   << "\r\n";
    body << "Bid Order  : " << trade.bidOrderId << "\r\n";
    body << "Ask Order  : " << trade.askOrderId << "\r\n";
    body << "Timestamp  : " << timeBuf          << "\r\n";

    std::cout << "[NotificationService] Sending email to " << smtpConfig_.recipient
              << " | subject: " << subject << "\n";

    EmailHelper::sendEmail(smtpConfig_, subject, body.str());

    std::cout << "[NotificationService] Email sent successfully for trade: " << trade.id << "\n";
}
