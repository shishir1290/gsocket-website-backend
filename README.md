# gsocketio Live Telemetry & Simulation Platform

A high-performance, real-time live simulation testbed and telemetry streaming engine powered by **[github.com/shishir1290/gsocketio](https://github.com/shishir1290/gsocketio)** — the zero-dependency, pure-Go Socket.IO v4 server.

![gsocketio telemetry](https://img.shields.io/badge/gsocketio-v1.0.4-blue?style=flat-square)
![Go Version](https://img.shields.io/badge/go-1.25+-cyan?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)

---

## ⚡ Overview

This project provides an end-to-end demonstration and testing environment for:
1. **Real-time Live Telemetry Streaming**: Low-latency event streaming (sub-millisecond) over RFC 6455 WebSockets and HTTP long-polling fallback.
2. **Multi-Node Fleet Simulation**: Dynamic virtual IoT/Drone/Server nodes generating simulated metrics (CPU load, temperature, battery, latency, sequence numbers).
3. **Interactive Control Deck**: Dynamic fleet scaling, burst traffic generation (500+ pkts/s), frequency tuning, and anomaly injection.
4. **Embedded Web UI**: Dark-mode glassmorphic telemetry dashboard embedded directly into the Go binary (`go:embed`).
5. **CLI Load Testing Client**: Concurrent stress testing client to benchmark packets per second and verify stability under load.

---

## 🚀 Quick Start

### 1. Prerequisites
- Go 1.20+ (Tested with Go 1.25)
- Git

### 2. Dependency
The project uses `github.com/shishir1290/gsocketio@latest`:
```bash
go get github.com/shishir1290/gsocketio@latest
```

### 3. Run the Telemetry Server & Dashboard

```bash
cd telemetry-simulation
go run main.go
```

The server will start and output:
```text
⚡ Starting gsocketio Realtime Live Telemetry & Simulation Platform
🚀 Web Dashboard: http://localhost:8080
⚡ Socket.IO Endpoint: ws://localhost:8080/socket.io/?EIO=4&transport=websocket
📊 REST API: http://localhost:8080/api/metrics
```

Open your browser at **[http://localhost:8080](http://localhost:8080)** to view the live dashboard.

#### Optional CLI Flags:
```bash
go run main.go -port=9000 -nodes=16 -autostart=true
```

---

## 🧪 Run Standalone CLI Load Simulation

In a separate terminal, run the standalone client load test:

```bash
# Run 10 concurrent simulated edge nodes sending telemetry for 15 seconds
go run ./cmd/client -server=http://localhost:8080 -clients=10 -duration=15s -interval=200ms
```

You will see live throughput and packet counts:
```text
⏱  Elapsed: 1s   | Sent: 50     pkts | Rate: 50   pkts/s | Errors: 0
⏱  Elapsed: 5s   | Sent: 250    pkts | Rate: 50   pkts/s | Errors: 0
⏱  Elapsed: 10s  | Sent: 500    pkts | Rate: 50   pkts/s | Errors: 0
==================================================================
🏁 Load Simulation Test Completed!
   Total Packets Sent: 750
   Total Errors:       0
   Average Throughput: 50.0 pkts/second
   Duration:           15.00s
==================================================================
```

---

## 📡 Socket.IO Event Reference

### Telemetry Events (Server ➔ Client)

| Event Name | Direction | Payload | Description |
| :--- | :--- | :--- | :--- |
| `telemetry:nodes_init` | Server ➔ Client | `[]DeviceTelemetry` | Initial snapshot of all simulated nodes |
| `telemetry:node_update` | Server ➔ Room(`dashboard`) | `DeviceTelemetry` | Continuous telemetry update per node |
| `telemetry:metrics` | Server ➔ Room(`dashboard`) | `ServerMetrics` | Global system metrics snapshot emitted every 1s |
| `telemetry:alert` | Server ➔ Namespace(`/`) | `AlertEvent` | Real-time incident or critical anomaly notification |
| `telemetry:pong` | Server ➔ Client | `any` | Echo of client timestamp for latency calculation |

### Control & Ingestion Events (Client ➔ Server)

| Event Name | Direction | Payload | Description |
| :--- | :--- | :--- | :--- |
| `telemetry:subscribe` | Client ➔ Server | `{ "client": "..." }` | Subscribes socket to `dashboard` room |
| `telemetry:ping` | Client ➔ Server | `timestamp` | Measures round-trip network latency |
| `telemetry:ingest` | Client ➔ Server | `DeviceTelemetry` | Ingests telemetry from external edge clients |
| `simulation:toggle` | Client ➔ Server | `{ "active": bool }` | Starts or pauses simulated telemetry generation |
| `simulation:scale` | Client ➔ Server | `{ "count": int }` | Scales number of active simulated nodes (1 - 200) |
| `simulation:interval` | Client ➔ Server | `{ "intervalMs": int }` | Sets telemetry generation tick frequency |
| `simulation:burst` | Client ➔ Server | `{ "count": int }` | Simulates a sudden spike of N packets |
| `simulation:alert_trigger`| Client ➔ Server | `{}` | Injects an anomaly event on a random node |

---

## 📁 Directory Structure

```
telemetry-simulation/
├── go.mod                     # Go module definitions (gsocketio v1.0.4)
├── go.sum
├── main.go                    # Server, Socket.IO routes, embedded assets & background tickers
├── internal/
│   ├── telemetry/
│   │   ├── metrics.go         # Collector for packets/s, memory stats, goroutines, latency
│   │   └── node.go            # Device and alert data models
│   └── simulator/
│       └── engine.go          # Multi-device simulation loop & chaos injection
├── web/
│   ├── index.html             # High-tech telemetry dashboard UI
│   ├── style.css              # Cyberpunk dark mode & glassmorphic styling
│   └── app.js                 # Realtime Socket.IO client visualization & control
├── cmd/
│   └── client/
│       └── main.go            # Standalone CLI stress test & simulator client
└── README.md                  # Comprehensive documentation
```

---

## 📊 REST API

In addition to Socket.IO, an HTTP endpoint is available for monitoring:

- **GET `/api/metrics`**:
  ```json
  {
    "uptimeSeconds": 42.1,
    "activeClients": 3,
    "simulatedNodes": 8,
    "totalPacketsIn": 672,
    "totalPacketsOut": 1344,
    "packetsPerSecIn": 16,
    "packetsPerSecOut": 32,
    "allocMB": 1.45,
    "totalAllocMB": 3.89,
    "sysMB": 12.3,
    "numGC": 1,
    "goroutines": 14,
    "avgLatencyMs": 0.22,
    "simulationActive": true,
    "timestamp": "2026-09-22T16:20:00Z"
  }
  ```

---

## 📜 License
MIT License. Powered by [github.com/shishir1290/gsocketio](https://github.com/shishir1290/gsocketio).
