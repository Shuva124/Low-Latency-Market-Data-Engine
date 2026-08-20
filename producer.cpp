#include "common.hpp"
#include <random>
#include <cstring>

constexpr int PORT = 9000;
constexpr int TICKS_PER_SYMBOL = 300000; // 3 symbols x 300k = 900,000 ticks total

void ticker_worker(const char* symbol, double initial_price, SimpleQueue<MarketTick>* queue) {
    std::mt19937 rng(1337 + static_cast<unsigned int>(std::hash<std::string>{}(symbol)));
    std::normal_distribution<double> price_delta(0.0, initial_price * 0.0005);
    std::uniform_real_distribution<double> qty_dist(0.1, 10.0);
    std::uniform_int_distribution<int> side_dist(0, 1);

    double current_price = initial_price;
    std::vector<MarketTick> batch;
    batch.reserve(512);

    for (uint32_t i = 1; i <= TICKS_PER_SYMBOL; ++i) {
        MarketTick tick{};
        tick.timestamp_ns = get_time_ns();
        std::strncpy(tick.symbol, symbol, sizeof(tick.symbol) - 1);

        current_price += price_delta(rng);
        if (current_price <= 1.0) current_price = initial_price;

        tick.price = current_price;
        tick.quantity = qty_dist(rng);
        tick.sequence_id = i;
        tick.side = static_cast<uint8_t>(side_dist(rng));

        batch.push_back(tick);
        if (batch.size() >= 512 || i == TICKS_PER_SYMBOL) {
            queue->push_batch(batch);
            batch.clear();
        }
    }
}

void network_tx_worker(SOCKET socket, SimpleQueue<MarketTick>* queue, uint64_t total_expected) {
    std::vector<MarketTick> batch;
    batch.reserve(2048);
    uint64_t total_sent = 0;

    while (total_sent < total_expected) {
        batch.clear();
        if (queue->pop_batch(batch, 2048) && !batch.empty()) {
            size_t bytes_to_send = batch.size() * sizeof(MarketTick);
            const char* ptr = reinterpret_cast<const char*>(batch.data());
            while (bytes_to_send > 0) {
                int sent = send(socket, ptr, static_cast<int>(bytes_to_send), 0);
                if (sent <= 0) break;
                ptr += sent;
                bytes_to_send -= sent;
            }
            total_sent += batch.size();
        }
    }
    std::cout << "[Producer] Network TX complete. Sent " << total_sent << " ticks." << std::endl;
}

int main() {
    SocketHelper::init();

    std::cout << "========================================================\n";
    std::cout << "       PROCESS 1: MARKET DATA ENGINE (PRODUCER)         \n";
    std::cout << "========================================================\n";

    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    int nodelay = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_addr.sin_port = htons(PORT);

    std::cout << "[Producer] Connecting to 127.0.0.1:" << PORT << "..." << std::endl;
    while (connect(sock, reinterpret_cast<SOCKADDR*>(&server_addr), sizeof(server_addr)) == SOCKET_ERROR) {
        Sleep(200);
    }
    std::cout << "[Producer] Connected to Engine server!\n" << std::endl;

    SimpleQueue<MarketTick> tx_queue;
    uint64_t total_expected = static_cast<uint64_t>(TICKS_PER_SYMBOL) * 3;

    auto start_time = std::chrono::high_resolution_clock::now();

    // 4 Producer Threads: BTC, ETH, SOL workers + 1 Network TX thread
    Thread btc_t(ticker_worker, "BTC-USD", 65000.0, &tx_queue);
    Thread eth_t(ticker_worker, "ETH-USD", 3500.0, &tx_queue);
    Thread sol_t(ticker_worker, "SOL-USD", 145.0, &tx_queue);
    Thread tx_t(network_tx_worker, sock, &tx_queue, total_expected);

    btc_t.join();
    eth_t.join();
    sol_t.join();
    tx_t.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() / 1000.0;

    std::cout << "[Producer] Finished in " << elapsed << " seconds." << std::endl;

    closesocket(sock);
    SocketHelper::cleanup();
    return 0;
}
