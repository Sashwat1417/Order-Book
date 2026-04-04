# Order Book System — High Level Design & Low Level Design

---

# Part 1 — High Level Design (HLD)

## System Overview

The system is split into two independent services that communicate via Kafka:

1. **order-service** — REST API, accepts and persists orders
2. **matching-engine** — Kafka consumer, matches orders, records trades

---

## Architecture Diagram

```mermaid
flowchart TD
    Client([Client / Trader])

    subgraph order-service
        API[REST API\nOrderController]
        SVC[OrderService]
        REPO[OrderRepository]
        KP[KafkaProducer]
    end

    subgraph Kafka
        NO[Topic: new-orders]
        TR[Topic: trades]
    end

    subgraph matching-engine
        ME[Matching Engine\nKafka Consumer]
        BOOK[In-Memory Order Book]
    end

    subgraph MongoDB
        BIDS[(bids collection)]
        ASKS[(asks collection)]
        TRADES[(trades collection)]
    end

    Client -->|POST /order| API
    Client -->|GET /orderbook| API
    Client -->|DELETE /order/:id| API

    API --> SVC
    SVC --> REPO
    SVC --> KP

    REPO --> BIDS
    REPO --> ASKS

    KP --> NO

    NO --> ME
    ME --> BOOK
    BOOK -->|trade executed| TR
    BOOK -->|update status| BIDS
    BOOK -->|update status| ASKS
    TR --> TRADES
```

---

## Order Placement Flow

```mermaid
sequenceDiagram
    participant Client
    participant API as order-service
    participant MongoDB
    participant Kafka

    Client->>API: POST /order {side, price, qty}
    API->>API: Validate request
    API->>MongoDB: Insert order (bids or asks)
    MongoDB-->>API: Acknowledged
    API->>Kafka: Publish to new-orders topic
    Kafka-->>API: Acknowledged
    API-->>Client: 201 Created {orderId}
```

---

## Order Matching Flow

```mermaid
sequenceDiagram
    participant Kafka
    participant ME as matching-engine
    participant Book as In-Memory Book
    participant MongoDB
    participant TradesTopic as Kafka trades

    Kafka->>ME: Consume message from new-orders
    ME->>Book: Add order to book
    Book->>Book: Check best bid >= best ask?
    alt Match found
        Book->>MongoDB: Update order status (FILLED / PARTIALLY_FILLED)
        Book->>MongoDB: Insert trade record
        Book->>TradesTopic: Publish trade event
    else No match
        Book->>Book: Order stays in book
    end
    ME->>Kafka: Commit offset
```

---

## Data Flow Summary

```mermaid
flowchart LR
    A[Client] -->|REST| B[order-service]
    B -->|persist| C[(MongoDB)]
    B -->|publish| D[Kafka: new-orders]
    D -->|consume| E[matching-engine]
    E -->|update| C
    E -->|publish| F[Kafka: trades]
```

---

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Async matching | Kafka | Decouples API from matching logic; orders not lost on crash |
| Separate bid/ask collections | Yes | Different sort orders; cleaner queries |
| In-memory order book | Yes | O(log n) matching; rebuilt from MongoDB on crash |
| Single Kafka partition | Yes | Guarantees ordering of messages (V1 simplicity) |
| Config via env vars | Yes | 12-factor app, no secrets in code |

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| OrderController | HTTP routing, request/response formatting |
| OrderService | Business logic, validation, orchestration |
| OrderRepository | MongoDB CRUD operations |
| KafkaProducer | Publish order events to Kafka |
| matching-engine | Consume orders, match, persist trades |

---

# Part 2 — Low Level Design (LLD)

## order-service Internal Structure

```mermaid
classDiagram
    class OrderController {
        -OrderService& service_
        +registerRoutes(httplib::Server&)
        -POST /order
        -GET /orderbook
        -DELETE /order/:id
    }

    class OrderService {
        -OrderRepository& repository_
        -KafkaProducer& kafkaProducer_
        +placeOrder(json body) json
        +getOrderBook() json
        +cancelOrder(string id) bool
    }

    class OrderRepository {
        -mongocxx::collection bids_
        -mongocxx::collection asks_
        +insertOrder(Order&)
        +findOpenBids() json
        +findOpenAsks() json
        +cancelOrder(string id) bool
    }

    class KafkaProducer {
        -RdKafka::Producer producer_
        -string topic_
        +publish(string message) bool
    }

    class Order {
        +string id
        +Side side
        +double price
        +double quantity
        +double remaining
        +OrderStatus status
        +int64_t timestamp
        +toJson() json
        +fromJson(json) Order
    }

    class Trade {
        +string id
        +string bidOrderId
        +string askOrderId
        +double price
        +double quantity
        +int64_t timestamp
        +toJson() json
    }

    OrderController --> OrderService
    OrderService --> OrderRepository
    OrderService --> KafkaProducer
    OrderRepository --> Order
    OrderService --> Order
```

---

## MongoDB Schema

### bids / asks collections
```json
{
  "_id":       "ObjectId",
  "id":        "1775248390193-3222866137",
  "price":     100.5,
  "quantity":  10,
  "remaining": 10,
  "status":    "OPEN",
  "timestamp": 1775248390193
}
```

### trades collection
```json
{
  "_id":        "ObjectId",
  "id":         "trade-abc123",
  "bidOrderId": "1775248390193-3222866137",
  "askOrderId": "1775248271288-643784549",
  "price":      100.5,
  "quantity":   5,
  "timestamp":  1775248400000
}
```

---

## In-Memory Order Book Structure (matching-engine)

```mermaid
flowchart TD
    subgraph Bids [Bids - sorted price DESC]
        B1[price: 102.0 → deque: order_a, order_b]
        B2[price: 101.5 → deque: order_c]
        B3[price: 100.0 → deque: order_d, order_e]
    end

    subgraph Asks [Asks - sorted price ASC]
        A1[price: 101.0 → deque: order_f]
        A2[price: 102.5 → deque: order_g, order_h]
        A3[price: 103.0 → deque: order_i]
    end

    MATCH{Best Bid 102.0 >= Best Ask 101.0\n→ MATCH!}

    B1 --> MATCH
    A1 --> MATCH
```

### C++ Data Structure
```cpp
// Bids: highest price first
map<double, deque<Order>, greater<double>> bids;

// Asks: lowest price first
map<double, deque<Order>> asks;
```

---

## Matching Algorithm

```mermaid
flowchart TD
    START([New order consumed from Kafka])
    ADD[Add to in-memory book]
    CHECK{best_bid.price >= best_ask.price?}
    FILL[Calculate fill qty = min of remaining on each side]
    UPDATE[Update remaining qty on both orders]
    TRADE[Create Trade record]
    PERSIST[Write trade to MongoDB]
    PUBLISH[Publish trade to Kafka: trades]
    REMOVE_BID{bid fully filled?}
    REMOVE_ASK{ask fully filled?}
    DONE([Done])

    START --> ADD
    ADD --> CHECK
    CHECK -->|No| DONE
    CHECK -->|Yes| FILL
    FILL --> UPDATE
    UPDATE --> TRADE
    TRADE --> PERSIST
    PERSIST --> PUBLISH
    PUBLISH --> REMOVE_BID
    REMOVE_BID -->|Yes| REMOVE_ASK
    REMOVE_BID -->|No| CHECK
    REMOVE_ASK -->|Yes| CHECK
    REMOVE_ASK -->|No| CHECK
```

---

## API Contracts

### POST /order
**Request:**
```json
{ "side": "BID", "price": 100.5, "quantity": 10 }
```
**Response 201:**
```json
{
  "id": "1775248390193-3222866137",
  "side": "BID",
  "price": 100.5,
  "quantity": 10,
  "remaining": 10,
  "status": "OPEN",
  "timestamp": 1775248390193
}
```

### GET /orderbook
**Response 200:**
```json
{
  "bids": [{ "id": "...", "price": 101.0, "quantity": 10, "remaining": 10, "status": "OPEN", "timestamp": 123 }],
  "asks": [{ "id": "...", "price": 102.0, "quantity": 5,  "remaining": 5,  "status": "OPEN", "timestamp": 456 }]
}
```

### DELETE /order/:id
**Response 200:** `{ "message": "Order cancelled" }`
**Response 404:** `{ "error": "Order not found" }`

---

## Kafka Message Formats

### new-orders topic
```json
{ "id": "...", "side": "ASK", "price": 101.0, "quantity": 5, "remaining": 5, "status": "OPEN", "timestamp": 1775248390193 }
```

### trades topic
```json
{ "id": "trade-abc123", "bidOrderId": "...", "askOrderId": "...", "price": 100.5, "quantity": 5, "timestamp": 1775248400000 }
```

---

## Error Handling

| Layer | Error | HTTP Response |
|---|---|---|
| Controller | Invalid JSON | 400 Bad Request |
| Controller | Missing fields | 400 Bad Request |
| Service | Validation failure | 400 Bad Request |
| Service | DB insert failed | 500 Internal Server Error |
| Service | Kafka publish failed | 500 Internal Server Error |
| Controller | Order not found (cancel) | 404 Not Found |

---

# Part 3 — Matching Engine Design

*This section will be completed once the matching engine implementation is done.*

Topics to be documented here:
- Kafka consumer setup and offset management
- Crash recovery / state rebuild from MongoDB
- Concurrency model
