#pragma once

#include <string>
#include <unordered_map>
#include <deque>
#include <utility>
#include <cstdint>

// Deduplicates order IDs using Redis SET NX PX — survives restarts and
// works across multiple matching-engine instances.
//
// Java analogy: like a Redis-backed @Cacheable(condition = "!#result") check,
// where the cache entry expires after a TTL and acts as a seen-ID registry.
//
// Fallback: if Redis is unreachable, falls back to the in-memory TTL cache
// that was previously inside OrderBook, and logs a warning.
class RedisDeduplicator {
public:
    RedisDeduplicator(std::string host, int port, int64_t ttlMs, std::string password = {});
    ~RedisDeduplicator();

    // Returns true  → first time this id is seen; marks it in Redis.
    // Returns false → duplicate; caller should skip processing.
    bool tryMarkSeen(const std::string& id);

private:
    std::string host_;
    int port_;
    int64_t ttlMs_;
    std::string password_;
    int sockFd_{-1};
    bool warnedRedisDown_{false};

    // In-memory fallback — activated only when Redis is down.
    std::unordered_map<std::string, int64_t> fallbackIds_;  // id -> seenAtMs
    std::deque<std::pair<std::string, int64_t>> fallbackQueue_;

    static constexpr int64_t kFallbackTtlMs  = 24LL * 60 * 60 * 1000;
    static constexpr size_t  kFallbackMaxSize = 1'000'000;
    static constexpr auto    kKeyPrefix       = "order_dedupe:";

    bool ensureConnected();
    void closeSocket();
    bool sendCommand(const std::deque<std::string>& args, std::string& firstLineOut);
    static std::string encodeResp(const std::deque<std::string>& args);
    static bool readLine(int fd, std::string& lineOut);
    static bool readExact(int fd, size_t n, std::string& out);
    bool authIfNeeded();

    bool  tryMarkSeenFallback(const std::string& id);
    void  evictFallback(int64_t nowMs);
    static int64_t nowMs();
};
