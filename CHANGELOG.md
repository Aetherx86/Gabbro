# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-11-08

### Added
- Initial release of Gabbro discrete-event Raft consensus engine and seeded chaos simulator in Go.
- Complete Raft consensus state machine implementing leader election (§5.2), log replication (§5.3), and safety properties (§5.4).
- Deterministic discrete-event simulation kernel decoupled from OS physical wall-clock time and network sockets.
- 64-bit PRNG-seeded chaos injection engine modeling network partitions, packet drop rates, and variable latency jitter.
- Mechanical invariant proof engine verifying Election Safety, Log Matching, Leader Completeness, and State Machine Consistency across thousands of randomized network faults.
- Dual procedure execution: `--mode simulate` (deterministic chaos replay & formal verification) vs `--mode benchmark` (high-throughput in-memory clustering).
