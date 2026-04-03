# Order Book

## Overview

An **order book** is a real-time, continuously updated record of all outstanding buy and sell orders for a financial instrument (stocks, futures, cryptocurrencies, etc.) on an exchange or trading venue.

It acts as the central mechanism through which buyers and sellers interact, providing transparency into market depth and price discovery.

---

## Structure

An order book is divided into two sides:

- **Bid side** — buy orders, sorted from highest price to lowest (buyers willing to pay the most are at the top)
- **Ask side** — sell orders, sorted from lowest price to highest (sellers willing to accept the least are at the top)

The difference between the best bid and the best ask is called the **spread**.

---

## How It Works

1. A trader places a **limit order** specifying a price and quantity.
2. The order enters the book and waits until a matching order on the opposite side arrives.
3. When a **market order** arrives, it matches against the best available price on the opposite side.
4. Matched orders result in a **trade** (execution), removing liquidity from the book.
5. Unmatched orders remain in the book until filled, cancelled, or expired.

---

## Key Concepts

| Term | Description |
|------|-------------|
| **Limit Order** | An order to buy/sell at a specific price or better |
| **Market Order** | An order to buy/sell immediately at the best available price |
| **Best Bid** | Highest price a buyer is currently willing to pay |
| **Best Ask** | Lowest price a seller is currently willing to accept |
| **Spread** | Difference between best bid and best ask |
| **Depth** | Volume of orders available at each price level |
| **Price Level** | A specific price point aggregating all orders at that price |
| **Matching Engine** | The system that pairs buy and sell orders to execute trades |

---

## Price-Time Priority

Most order books follow **FIFO (First In, First Out)** matching within a price level:

- Orders at the same price are filled in the order they were received.
- Better-priced orders are always filled before worse-priced ones.

---

## Use Cases

- **Price discovery** — reflects the current consensus value of an asset
- **Liquidity assessment** — shows how easily large orders can be absorbed
- **Algorithmic trading** — strategies like market making, arbitrage, and HFT rely on real-time order book data
- **Risk management** — traders use book depth to estimate slippage and market impact
