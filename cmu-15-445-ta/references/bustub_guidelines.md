# BusTub Development Guidelines & Common Pitfalls

## 1. Memory Management
- **Page Pinning**: Every `FetchPage` must be paired with an `UnpinPage`.
- **RAII**: Use smart pointers.

## 2. Concurrency
- **Latch Crabbing**: Follow the standard crabbing protocol in B+ Tree.
- **Deadlocks**: Standardize acquisition order.

## 3. Hash Join
- **NULL Consistency**: Ensure hash and equality handles NULLs identically.
