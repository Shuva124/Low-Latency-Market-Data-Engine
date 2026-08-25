#include "common.hpp"
#include <unordered_map>

constexpr int PORT = 9000;
constexpr uint64_t TOTAL_EXPECTED = 900000;

struct Stat {
    uint64_t ticks{0};
    double px_vol{0.0};
    double vol{0.0};
    double min_px{1e18};
    double max_px{0.0};
    double bid{0.0};
    double ask{0.0};
};

void tcp_rx_worker(SOCKET client_sock, SimpleQueue<MarketTick>* rx_queue, std::atomic<bool>* done) {
    std::vector<MarketTick> buffer(1024);
    uint64_t total = 0;

    while (total < TOTAL_EXPECTED) {
        size_t chunk = std::min<size_t>(1024, TOTAL_EXPECTED - total);
        char* ptr = reinterpret_cast<char*>(buffer.data());
        size_t bytes_left = chunk * sizeof(MarketTick);

        bool sock_error = false;
        while (bytes_left > 0) {
            int rec = recv(client_sock, ptr, static_cast<int>(bytes_left), 0);
            if (rec <= 0) {
                sock_error = true;
                break;
            }
            ptr += rec;
            bytes_left -= rec;
        }

        if (sock_error) break;

        std::vector<MarketTick> batch(buffer.begin(), buffer.begin() + chunk);
        rx_queue->push_batch(batch);
        total += chunk;
    }

    done->store(true);
    rx_queue->shutdown();
    std::cout << "[Engine] Received all " << total << " ticks." << std::endl;
}

void analytics_worker(SimpleQueue<MarketTick>* rx_queue, std::unordered_map<std::string, Stat>* stats, std::atomic<uint64_t>* processed, std::atomic<uint64_t>* total_lat) {
    std::vector<MarketTick> batch;
    batch.reserve(2048);

    while (true) {
        batch.clear();
        if (!rx_queue->pop_batch(batch, 2048)) break;

        uint64_t now_ns = get_time_ns();
        for (const auto& tick : batch) {
            auto& s = (*stats)[tick.symbol];
            s.ticks++;
            s.px_vol += (tick.price * tick.quantity);
            s.vol += tick.quantity;
            s.min_px = std::min(s.min_px, tick.price);
            s.max_px = std::max(s.max_px, tick.price);
            if (tick.side == 0) s.bid = tick.price; else s.ask = tick.price;

            if (now_ns >= tick.timestamp_ns) {
                total_lat->fetch_add(now_ns - tick.timestamp_ns, std::memory_order_relaxed);
            }
            processed->fetch_add(1, std::memory_order_relaxed);
        }
    }
}

int main() {
    SocketHelper::init();

    std::cout << "========================================================\n";
    std::cout << "      PROCESS 2: ANALYTICS & ORDER ENGINE (CONSUMER)    \n";
    std::cout << "========================================================\n";

    SOCKET server_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    int reuse = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(PORT);

    bind(server_sock, reinterpret_cast<SOCKADDR*>(&addr), sizeof(addr));
    listen(server_sock, SOMAXCONN);

    std::cout << "[Engine] Server listening on port " << PORT << "..." << std::endl;
    SOCKET client_sock = accept(server_sock, nullptr, nullptr);
    int nodelay = 1;
    setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    std::cout << "[Engine] Producer client connected!\n" << std::endl;

    SimpleQueue<MarketTick> rx_queue;
    std::unordered_map<std::string, Stat> stats;
    std::atomic<bool> done{false};
    std::atomic<uint64_t> processed{0};
    std::atomic<uint64_t> total_lat{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // 3 Engine Threads: TCP Rx thread, Analytics worker, Stats reporter
    std::thread rx_t(tcp_rx_worker, client_sock, &rx_queue, &done);
    std::thread analytics_t(analytics_worker, &rx_queue, &stats, &processed, &total_lat);

    rx_t.join();
    analytics_t.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() / 1000000.0;

    // Output Analytics Summary
    std::cout << "\n========================================================\n";
    std::cout << "                 REAL-TIME ANALYTICS SUMMARY            \n";
    std::cout << "========================================================\n";
    std::cout << std::left << std::setw(10) << "Symbol"
              << std::right << std::setw(12) << "Total Ticks"
              << std::setw(15) << "VWAP ($)"
              << std::setw(14) << "Spread ($)"
              << std::setw(15) << "Min Price ($)"
              << std::setw(15) << "Max Price ($)" << "\n";
    std::cout << "--------------------------------------------------------\n";

    for (const auto& pair : stats) {
        const auto& sym = pair.first;
        const auto& s = pair.second;
        double vwap = s.vol > 0 ? s.px_vol / s.vol : 0.0;
        double spread = (s.ask > 0 && s.bid > 0) ? std::abs(s.ask - s.bid) : 0.0;

        std::cout << std::left << std::setw(10) << sym
                  << std::right << std::setw(12) << s.ticks
                  << std::setw(15) << std::fixed << std::setprecision(2) << vwap
                  << std::setw(14) << std::setprecision(4) << spread
                  << std::setw(15) << std::setprecision(2) << s.min_px
                  << std::setw(15) << std::setprecision(2) << s.max_px << "\n";
    }

    // Output Benchmark Report
    uint64_t total_msg = processed.load();
    double throughput = elapsed > 0 ? (total_msg / elapsed) : 0.0;
    double avg_lat = total_msg > 0 ? ((total_lat.load() / (double)total_msg) / 1000.0) : 0.0;

    std::cout << "\n========================================================\n";
    std::cout << "             PERFORMANCE BENCHMARK REPORT               \n";
    std::cout << "========================================================\n";
    std::cout << std::left << std::setw(30) << " Total Messages Processed:"
              << std::right << std::setw(24) << total_msg << "\n";
    std::cout << std::left << std::setw(30) << " Execution Time:"
              << std::right << std::setw(20) << std::fixed << std::setprecision(4) << elapsed << " s\n";
    std::cout << std::left << std::setw(30) << " Peak Throughput:"
              << std::right << std::setw(18) << std::fixed << std::setprecision(1) << throughput << " msg/sec\n";
    std::cout << std::left << std::setw(30) << " Average End-to-End Latency:"
              << std::right << std::setw(20) << std::fixed << std::setprecision(2) << avg_lat << " us\n";
    std::cout << "========================================================\n\n";

    closesocket(client_sock);
    closesocket(server_sock);
    SocketHelper::cleanup();
    return 0;
}
