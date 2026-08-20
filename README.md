# 100% Pure C++ Concurrent Market Data Processor

A high-throughput, low-latency market data processing pipeline built entirely in standard **C++17**. Designed to simulate the backend of a high-frequency financial exchange, this system decouples data ingestion from real-time analytics using TCP sockets for Inter-Process Communication (IPC), a native Win32/C++ thread synchronization model, and a pure C++ process orchestrator.

---

## 🏛️ System Architecture

The system is 100% native C++, comprising **7 concurrent threads** across two isolated worker processes (`producer.exe` and `engine.exe`), managed by a native C++ orchestrator (`runner.exe`).

```
                   ┌──────────────────────────────────────────────┐
                   │        Pure C++ Process Orchestrator         │
                   │        (bin/runner.exe - C++17)              │
                   └──────────────────────┬───────────────────────┘
                                          │
                  ┌───────────────────────┴───────────────────────┐
                  │ Win32 CreateProcessA                          │ Win32 CreateProcessA
                  ▼                                               ▼
┌──────────────────────────────────┐            ┌──────────────────────────────────┐
│        PROCESS 1: PRODUCER       │            │         PROCESS 2: ENGINE        │
│       (Market Data Engine)       │            │        (Analytics & Order)       │
│                                  │  TCP IPC   │                                  │
│ Thread 1: BTC worker             │ ─────────> │ Thread 1: TCP Rx listener        │
│ Thread 2: ETH worker             │ (127.0.0.1)│ Thread 2: Analytics engine       │
│ Thread 3: SOL worker             │            │ Thread 3: Latency & stats        │
│ Thread 4: Network TX dispatcher  │            │                                  │
└──────────────────────────────────┘            └──────────────────────────────────┘
```

---

## ⚡ Key Technical Features

1. **100% Pure C++ Core**:
   - Written strictly in standard C++17. All process management, thread handling, synchronization, binary packet streaming, analytics calculation, and live output streaming are executed natively in compiled C++.

2. **Inter-Process Communication (IPC)**:
   - Streams tightly packed 37-byte binary structs (`#pragma pack(push, 1)`) over local TCP sockets to eliminate serialization overhead.
   - Configures `TCP_NODELAY` to prevent Nagle buffering delays.

3. **Producer-Consumer Synchronization**:
   - Implements thread-safe message queues using native C++ wrappers (`Mutex`, `ConditionVariable`, `LockGuard`, `UniqueLock`) for sub-microsecond synchronization latency.

4. **Lock Contention Management & Performance**:
   - Uses `std::atomic` counters for lock-free peak throughput and nanosecond latency tracking across threads.
   - Micro-batched buffers (2048 ticks/batch) maximize CPU cache hit rates.

---

## 📊 Performance Benchmarks

Tested natively on Windows via MinGW (`g++ -std=c++17 -O3`):

| Metric | Benchmark Result |
| :--- | :--- |
| **Total Messages Processed** | **900,000** |
| **Peak Throughput** | **~1.75 Million msg/sec** |
| **Average End-to-End Latency** | **~143 µs** |
| **Execution Time** | **~0.51 seconds** |
| **Dropped Messages** | **0** (100% sequence continuity) |

---

## 📁 Repository Structure

```
d:\new_F\
├── include\
│   ├── MarketTick.hpp          # Tightly packed binary packet header (37 bytes)
│   ├── ThreadSafeQueue.hpp     # Synchronized Producer-Consumer thread-safe queue
│   ├── ThreadSync.hpp          # Native Win32 Mutex/CV/Thread wrappers for C++
│   ├── NetworkSocket.hpp       # TCP Socket abstraction with TCP_NODELAY
│   ├── AnalyticsEngine.hpp     # Real-time VWAP, Spread, Min/Max calculator
│   └── LatencyTracker.hpp      # Nanosecond latency tracker & atomic stats
├── src\
│   ├── producer_main.cpp       # Process 1: Market Data Engine (4 threads)
│   ├── engine_main.cpp         # Process 2: Analytics & Order Processing Engine (3 threads)
│   └── benchmark_runner.cpp    # Pure C++ Process Orchestrator & Benchmark Runner
├── scripts\
│   ├── build.ps1               # PowerShell build script
│   └── run_benchmark.ps1       # PowerShell benchmark runner
├── Makefile                    # Standard Makefile for g++
└── README.md                   # Complete system documentation
```

---

## 🚀 Quick Start Guide

### Option 1: Direct Pure C++ Execution (Recommended)
Run the native C++ build script and executable:
```powershell
.\scripts\build.ps1
.\bin\runner.exe
```

### Option 2: Using Makefile
```cmd
make
make run
```
