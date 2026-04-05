#include "RedisDeduplicator.hpp"

#include <chrono>
#include <iostream>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

RedisDeduplicator::RedisDeduplicator(std::string host, int port, int64_t ttlMs, std::string password)
    : host_(std::move(host)), port_(port), ttlMs_(ttlMs), password_(std::move(password)) {}

RedisDeduplicator::~RedisDeduplicator() {
    closeSocket();
}

bool RedisDeduplicator::tryMarkSeen(const std::string& id) {
    if (!ensureConnected()) {
        if (!warnedRedisDown_) {
            std::cerr << "[Dedup] Redis unreachable; falling back to in-memory dedupe.\n";
            warnedRedisDown_ = true;
        }
        return tryMarkSeenFallback(id);
    }

    std::string response;
    const std::string key = std::string(kKeyPrefix) + id;
    if (!sendCommand({"SET", key, "1", "NX", "PX", std::to_string(ttlMs_)}, response)) {
        closeSocket();
        if (!warnedRedisDown_) {
            std::cerr << "[Dedup] Redis command failed; falling back to in-memory dedupe.\n";
            warnedRedisDown_ = true;
        }
        return tryMarkSeenFallback(id);
    }

    // +OK => key set (first time); $-1 => nil (duplicate)
    if (!response.empty() && response[0] == '+') return true;
    if (!response.empty() && response[0] == '$') return false;

    // Any other reply type: be conservative and fall back (avoid double-processing).
    closeSocket();
    if (!warnedRedisDown_) {
        std::cerr << "[Dedup] Redis unexpected reply; falling back to in-memory dedupe.\n";
        warnedRedisDown_ = true;
    }
    return tryMarkSeenFallback(id);
}

bool RedisDeduplicator::ensureConnected() {
    if (sockFd_ >= 0) return true;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* res = nullptr;
    const std::string portStr = std::to_string(port_);
    if (getaddrinfo(host_.c_str(), portStr.c_str(), &hints, &res) != 0) return false;

    int fd = -1;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;

        timeval timeout{};
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        int one = 1;
        (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            sockFd_ = fd;
            break;
        }
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (sockFd_ < 0) return false;

    if (!authIfNeeded()) {
        closeSocket();
        return false;
    }

    warnedRedisDown_ = false;
    return true;
}

void RedisDeduplicator::closeSocket() {
    if (sockFd_ >= 0) {
        ::close(sockFd_);
        sockFd_ = -1;
    }
}

bool RedisDeduplicator::authIfNeeded() {
    if (password_.empty()) return true;
    std::string response;
    if (!sendCommand({"AUTH", password_}, response)) return false;
    return !response.empty() && response[0] == '+';
}

bool RedisDeduplicator::sendCommand(const std::deque<std::string>& args, std::string& firstLineOut) {
    firstLineOut.clear();
    if (sockFd_ < 0) return false;

    const std::string payload = encodeResp(args);
    size_t sent = 0;
    while (sent < payload.size()) {
        const ssize_t n = ::send(sockFd_, payload.data() + sent, payload.size() - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }

    // Read the first RESP line (enough to distinguish +OK vs $-1).
    return readLine(sockFd_, firstLineOut);
}

std::string RedisDeduplicator::encodeResp(const std::deque<std::string>& args) {
    std::string out;
    out.reserve(64 + args.size() * 16);
    out.append("*").append(std::to_string(args.size())).append("\r\n");
    for (const auto& a : args) {
        out.append("$").append(std::to_string(a.size())).append("\r\n");
        out.append(a).append("\r\n");
    }
    return out;
}

bool RedisDeduplicator::readLine(int fd, std::string& lineOut) {
    lineOut.clear();
    char c = '\0';
    bool gotCR = false;
    while (true) {
        const ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) return false;
        if (!gotCR) {
            if (c == '\r') {
                gotCR = true;
            } else {
                lineOut.push_back(c);
            }
        } else {
            if (c == '\n') break;
            // Unexpected sequence; include '\r' and continue.
            lineOut.push_back('\r');
            if (c != '\r') lineOut.push_back(c);
            gotCR = (c == '\r');
        }
    }
    return true;
}

bool RedisDeduplicator::readExact(int fd, size_t n, std::string& out) {
    out.clear();
    out.resize(n);
    size_t off = 0;
    while (off < n) {
        const ssize_t r = ::recv(fd, out.data() + off, n - off, 0);
        if (r <= 0) return false;
        off += static_cast<size_t>(r);
    }
    return true;
}

bool RedisDeduplicator::tryMarkSeenFallback(const std::string& id) {
    const int64_t seenAt = nowMs();
    evictFallback(seenAt);
    if (fallbackIds_.find(id) != fallbackIds_.end()) return false;
    fallbackIds_[id] = seenAt;
    fallbackQueue_.push_back({id, seenAt});
    evictFallback(seenAt);
    return true;
}

void RedisDeduplicator::evictFallback(int64_t nowMsValue) {
    while (!fallbackQueue_.empty()) {
        const auto& front = fallbackQueue_.front();
        const std::string& id = front.first;
        const int64_t seenAt = front.second;

        const bool ttlExpired = (nowMsValue - seenAt) > kFallbackTtlMs;
        const bool overLimit = fallbackIds_.size() > kFallbackMaxSize;
        if (!ttlExpired && !overLimit) break;

        auto it = fallbackIds_.find(id);
        if (it != fallbackIds_.end() && it->second == seenAt) fallbackIds_.erase(it);
        fallbackQueue_.pop_front();
    }
}

int64_t RedisDeduplicator::nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

