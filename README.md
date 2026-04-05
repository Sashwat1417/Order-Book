# Order Book

A distributed financial order book built in **C++17** — featuring a REST API for order placement, MongoDB persistence, and a Kafka-driven async matching engine.

> First project exploring distributed systems concepts in C++.

---

## What it does

- Accepts **buy (bid)** and **sell (ask)** limit orders via a REST API
- Persists orders to **MongoDB** and publishes them to **Kafka**
- A separate **matching engine** consumes orders asynchronously, matches them by price-time priority, and records trades

---

## Architecture

```
Client  →  order-service  →  MongoDB (bids / asks)
                          →  Kafka: new-orders
                                       ↓
                             matching-engine
                                       ↓
                             MongoDB (trades)  +  Kafka: trades
```

---

## Confluence Doc 

[link](https://eudoxus.atlassian.net/wiki/spaces/~7120208a67821615d04b3dad24d4b9b2b9f999/pages/393218/Order+Book?atlOrigin=eyJpIjoiNjRkMzkyNGQ4YzkwNDFmNjk1NGVlNWMzOTlmZTA2YWEiLCJwIjoiYyJ9)

---

## Tech Stack

![C++](https://img.shields.io/badge/C++-17-00599C?style=flat&logo=c%2B%2B)
![MongoDB](https://img.shields.io/badge/MongoDB-green?style=flat&logo=mongodb)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka)
![CMake](https://img.shields.io/badge/CMake-064F8C?style=flat&logo=cmake)

| Concern | Technology |
|---|---|
| HTTP Server | cpp-httplib |
| Database | MongoDB (mongocxx) |
| Messaging | Apache Kafka (librdkafka) |
| JSON | nlohmann/json |
| Build | CMake |

---

## API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/order` | Place a bid or ask order |
| `GET` | `/orderbook` | View open bids and asks |
| `DELETE` | `/order/:id` | Cancel an order |

---

## Running Locally

### 1. Install dependencies
```bash
brew install cmake mongo-cxx-driver librdkafka boost kafka
```

### 2. Start infrastructure
```bash
brew services start zookeeper
brew services start kafka
brew services start mongodb-community
```

### 3. Create Kafka topics _(first time only)_
```bash
kafka-topics --create --topic new-orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic trades --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Build
```bash
cmake -B build -S . -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build
```

### 5. Run
```bash
./build/order/order-service        # Terminal 1 — REST API on :3000
./build/matching-engine/matching-engine  # Terminal 2 — matching engine
```

### Environment variables
| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB URI |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker |
| `SERVER_PORT` | `3000` | API port |

---

## Documentation

| Doc | Description |
|---|---|
| [PRD](docs/PRD.md) | Requirements, scope, functional & non-functional specs |
| [HLD & LLD](docs/HLD-LLD.md) | Architecture, design decisions, data models, API contracts |
