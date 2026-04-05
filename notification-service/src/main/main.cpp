#include <iostream>
#include <csignal>
#include <atomic>
#include <cstdlib>
#include <stdexcept>

#include "listener/TradeListener.hpp"
#include "service/NotificationService.hpp"
#include "service/EmailHelper.hpp"

static std::atomic<bool> g_running{true};
static void signalHandler(int) { g_running = false; }

int main() {
    signal(SIGINT,  signalHandler);
    signal(SIGTERM, signalHandler);

    auto getenv_or = [](const char* key, const char* def) -> std::string {
        const char* val = std::getenv(key);
        return val ? val : def;
    };

    std::string brokers     = getenv_or("KAFKA_BROKERS",       "localhost:9092");
    std::string tradesTopic = getenv_or("KAFKA_TRADES_TOPIC",  "trades");
    std::string groupId     = getenv_or("KAFKA_GROUP_ID",      "notification-service-group");
    std::string gmailUser   = getenv_or("GMAIL_USER",          "");
    std::string appPassword = getenv_or("GMAIL_APP_PASSWORD",  "");
    std::string recipient   = getenv_or("NOTIFY_EMAIL",        "");

    if (gmailUser.empty() || appPassword.empty() || recipient.empty()) {
        std::cerr << "[NotificationService] GMAIL_USER, GMAIL_APP_PASSWORD,"
                     " and NOTIFY_EMAIL must be set.\n";
        return 1;
    }

    try {
        EmailHelper::SmtpConfig smtpConfig{gmailUser, appPassword, recipient};
        NotificationService service(smtpConfig);
        TradeListener       listener(brokers, tradesTopic, groupId);

        std::cout << "[NotificationService] Started. Listening on: " << tradesTopic << "\n";

        while (g_running) {
            listener.poll([&](const std::string& msg) {
                service.processTradeEvent(msg);
            });
        }

        std::cout << "[NotificationService] Shutting down gracefully.\n";
    } catch (const std::exception& e) {
        std::cerr << "[NotificationService] Fatal error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
