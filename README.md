# Chorus

[![crates.io](https://img.shields.io/crates/v/chorus-client.svg)](https://crates.io/crates/chorus-client)
[![docs.rs](https://docs.rs/chorus-client/badge.svg)](https://docs.rs/chorus-client)
[![Rust client source](https://img.shields.io/badge/source-rust%2Fclient-blue.svg)](rust/client)

Chorus is a single-writer write-ahead log built directly on Google Cloud
Storage. If your database already lives in GCP, three zonal Rapid buckets and
one regional bucket give you a durable, zone-fault-tolerant WAL with no
Apache Kafka, no etcd, and no extra servers to operate. The buckets *are* the log.

Building robust, scalable, and cheap storage on object stores comes at a cost:
write latency. Reads can hide behind a cache; durability-critical writes cannot.
Chorus closes that gap. A single Rapid bucket lives in only one availability
zone, so Chorus replicates the log across a strict majority of zonal buckets
(three in the common deployment), commits each record once any two report it
durable, and survives the loss of any one zone. Its entire control plane is a
single regional GCS object, a compare-and-swap register kept off the per-record
commit path, so commits run at zonal-bucket latency.

For 4 KiB records committed individually in-region, Chorus commits with a
1.71 ms median and 2.74 ms p99, both below the median of Google Cloud's
synchronously-replicated regional block storage. It delivers regional durability
at single-zone write latency, backed by a machine-checked correctness argument:
the protocol is specified and model-checked in P for safety and liveness, the
production Rust client is exercised under deterministic simulation testing, and
its traces are replayed against the P model with PObserve to check the
implementation against the specification.

To learn more about the story behind Chorus, check out the [introductory blog post][blog].

[blog]: https://rockwotj.com/blog/chorus
