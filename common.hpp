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

// 1. Tightly packed binary MarketTick struct (37 bytes)
#pragma pack(push, 1)
struct MarketTick {
    uint64_t timestamp_ns; // Nanosecond timestamp
    char symbol[8];        // "BTC-USD", "ETH-USD", "SOL-USD"
    double price;          // Price in USD
    double quantity;       // Volume size
    uint32_t sequence_id;  // Sequence counter
    uint8_t side;          // 0 = Buy/Bid, 1 = Sell/Ask
};
#pragma pack(pop)

// 2. High-precision nanosecond timer
inline uint64_t get_time_ns() {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

// 3. Simple Synchronization Primitives & Thread
class Mutex {
    CRITICAL_SECTION cs;
public:
    Mutex() { InitializeCriticalSection(&cs); }
    ~Mutex() { DeleteCriticalSection(&cs); }
    void lock() { EnterCriticalSection(&cs); }
    void unlock() { LeaveCriticalSection(&cs); }
    CRITICAL_SECTION* get_cs() { return &cs; }
};

class LockGuard {
    Mutex& m;
public:
    LockGuard(Mutex& mutex) : m(mutex) { m.lock(); }
    ~LockGuard() { m.unlock(); }
};

class ConditionVar {
    CONDITION_VARIABLE cv;
public:
    ConditionVar() { InitializeConditionVariable(&cv); }
    void notify_one() { WakeConditionVariable(&cv); }
    void notify_all() { WakeAllConditionVariable(&cv); }
    template <typename Predicate>
    void wait(Mutex& m, Predicate pred) {
        while (!pred()) {
            SleepConditionVariableCS(&cv, m.get_cs(), INFINITE);
        }
    }
};

class Thread {
    HANDLE hThread{NULL};
    struct Data { std::function<void()> func; };
    static DWORD WINAPI Proc(LPVOID p) {
        std::unique_ptr<Data> data(static_cast<Data*>(p));
        if (data && data->func) data->func();
        return 0;
    }
public:
    Thread() = default;
    template <typename F, typename... Args>
    explicit Thread(F&& f, Args&&... args) {
        auto bound = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        auto data = new Data{bound};
        hThread = CreateThread(NULL, 0, Proc, data, 0, NULL);
    }
    ~Thread() { if (hThread) CloseHandle(hThread); }
    void join() {
        if (hThread) {
            WaitForSingleObject(hThread, INFINITE);
            CloseHandle(hThread);
            hThread = NULL;
        }
    }
};

// 4. Thread-Safe Queue
template <typename T>
class SimpleQueue {
    std::queue<T> q;
    Mutex m;
    ConditionVar cv;
    bool done{false};
public:
    void push_batch(const std::vector<T>& items) {
        {
            LockGuard lock(m);
            for (const auto& item : items) q.push(item);
        }
        cv.notify_all();
    }

    bool pop_batch(std::vector<T>& batch, size_t max_items) {
        m.lock();
        cv.wait(m, [this] { return !q.empty() || done; });
        if (q.empty() && done) {
            m.unlock();
            return false;
        }
        while (!q.empty() && batch.size() < max_items) {
            batch.push_back(q.front());
            q.pop();
        }
        m.unlock();
        return true;
    }

    void shutdown() {
        { LockGuard lock(m); done = true; }
        cv.notify_all();
    }
};

// 5. TCP Socket Helper
struct SocketHelper {
    static void init() {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
    }
    static void cleanup() {
        WSACleanup();
    }
};

#endif // COMMON_HPP
