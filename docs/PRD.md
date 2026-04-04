# Order Book System — Product Requirements Document (PRD)

## Purpose
This document describes the requirements and goals for the distributed Order Book system.

---

## What is an Order Book?
An order book is a real-time record of all outstanding buy (bid) and sell (ask) orders for a financial instrument. It is the core mechanism of any exchange or trading venue — enabling price discovery and trade execution between buyers and sellers.

---

## Problem Statement
Build a backend system that:
- Accepts buy and sell orders via a REST API
- Persists orders durably
- Asynchronously matches bids against asks using price-time priority
- Records confirmed trades

---

## Scope (V1 — Single Asset)
This version handles a **single financial instrument** (e.g. one stock or one crypto pair). Multi-symbol support is deferred to V2.

---

## Key Requirements

### Functional Requirements
| # | Requirement |
|---|---|
| FR-1 | System shall accept limit orders (bid or ask) via REST API |
| FR-2 | Each order shall have a price, quantity, side (BID/ASK), and timestamp |
| FR-3 | Orders shall be persisted to MongoDB immediately upon receipt |
| FR-4 | Orders shall be published to a Kafka topic for async processing |
| FR-5 | Matching engine shall consume orders and match using price-time priority |
| FR-6 | A trade shall be recorded when bid price >= ask price |
| FR-7 | Clients shall be able to view the current order book (open bids and asks) |
| FR-8 | Clients shall be able to cancel an open order by ID |

### Non-Functional Requirements
| # | Requirement |
|---|---|
| NFR-1 | Order placement API response time < 100ms |
| NFR-2 | Orders must not be lost if matching engine crashes (Kafka offset replay) |
| NFR-3 | System must support restart and replay from Kafka from last committed offset |
| NFR-4 | Config (DB URI, Kafka brokers, ports) must be environment-variable driven |

---

## Order States

```
OPEN → PARTIALLY_FILLED → FILLED
OPEN → CANCELLED
```

---

## Out of Scope (V1)
- Market orders (only limit orders in V1)
- Multi-symbol / multi-asset support
- Authentication / authorization
- WebSocket real-time feed
- Order expiry (GTD, IOC, FOK)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | C++17 |
| REST API | cpp-httplib |
| Database | MongoDB |
| Messaging | Apache Kafka |
| Build | CMake |

---

## Team
- Developer: Sashwat
