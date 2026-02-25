# Phase 4: Analysis Notes

## PoW security benefit (Sybil resistance)

Proof-of-Work on HELLO increases the cost of fake joins: an attacker must compute a valid nonce (hash with k leading zero hex digits) per fake identity. This raises the cost to create many Sybil nodes compared to honest nodes that join once. Chosen k_pow trades off join latency vs. resistance (higher k = more work per join, slower honest joins).

## Effect on honest node join latency

When k_pow > 0, every HELLO (bootstrap and responses) requires computing PoW once. Join latency increases by the PoW generation time (see table below). For small k (e.g. 2–4), this is typically milliseconds.

## Justify chosen k_pow

- **k_pow = 0**: Default; disables PoW (backward compatible).
- **k_pow = 2–4**: Lightweight; sub-second join latency; raises cost for mass Sybil creation.
- Use `python simulator.py` to print the **PoW generation time table** for k_pow in (2, 3, 4); use that table/chart in your report.

## Hybrid push-pull: convergence and overhead

- Run: `python simulator.py` (uses N=10,20,50; seeds 10,20,30; both modes).
- Compare **push_only** vs **hybrid_push_pull**: convergence time (mean ± std) and message overhead (mean ± std).
- Overhead includes IHAVE and IWANT (all message types are counted in `total_messages` in the simulator).
- Convergence uses 95% threshold (same as Phase 3).

## Commands

- **Node (push_only, default):**  
  `python node.py --port 8000 --bootstrap 127.0.0.1:8001`
- **Node (hybrid):**  
  `python node.py --port 8000 --bootstrap 127.0.0.1:8001 --mode hybrid_push_pull --interval-pull 5 --ids-max-ihave 32`
- **Node with PoW:**  
  `python node.py --port 8000 --bootstrap 127.0.0.1:8001 --k-pow 4`
- **Simulator + PoW table:**  
  `python simulator.py`
- **Tests:**  
  `python -m unittest tests.test_phase4 -v`
