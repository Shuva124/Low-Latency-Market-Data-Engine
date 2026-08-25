#ifndef COMMON_HPP
#define COMMON_HPP

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <cstring>
#include <cstdint>

#include <mutex>
#include <condition_variable>
#include <thread>

#pragma pack(push, 1)

struct MarketTick {
    uint64_t timestamp_ns;
    char symbol[8];
    double price;
    double quantity;
    uint32_t sequence_id;
    uint8_t side;
};

#pragma pack(pop)

inline uint64_t get_time_ns() {
    const auto now = std::chrono::high_resolution_clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()
        ).count()
    );
}

template <typename T>
class SimpleQueue {
private:
    std::queue<T> q;
    mutable std::mutex m;
    std::condition_variable cv;
    bool done{false};

public:
    void push_batch(const std::vector<T>& items) {
        {
            std::lock_guard<std::mutex> lock(m);
            for (const auto& item : items) {
                q.push(item);
            }
        }
        cv.notify_all();
    }

    bool pop_batch(std::vector<T>& batch, size_t max_items) {
        std::unique_lock<std::mutex> lock(m);

        cv.wait(lock, [this] {
            return !q.empty() || done;
        });

        if (q.empty() && done) {
            return false;
        }

        while (!q.empty() && batch.size() < max_items) {
            batch.push_back(std::move(q.front()));
            q.pop();
        }

        return true;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(m);
            done = true;
        }

        cv.notify_all();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(m);
        return q.empty();
    }
};

struct SocketHelper {
    static bool init() {
        WSADATA wsa{};

        const int result = WSAStartup(
            MAKEWORD(2, 2),
            &wsa
        );

        if (result != 0) {
            std::cerr << "[Socket] WSAStartup failed: "
                      << result << '\n';
            return false;
        }

        return true;
    }
    static void cleanup() {
        WSACleanup();
    }
};

#endif