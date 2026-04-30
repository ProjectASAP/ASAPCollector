1. Distributed support by partitioning the data? for otel, telegraf
   _(2026-04-30: partial — single-host 2-node simulation landed in
   [#198](https://github.com/ProjectASAP/DataCollector/pull/198)
   `bench_2node_sim.sh`, validating per-node accounting, port-shifting,
   and balance math with hash-partitioned input. Real multi-node still
   pending — swap the two-binary launcher for ssh-spawn.)_
2. Batch processing support for otel, telegraf?
3. potential throughput improvement?
4. lossy or lossless delivery guarantees?
5. Comparison with Kafka?
6. out-of-ordering metric sampling handling in otel, telegraf?
7. Does otel, telegraf write data to spark/iceberg? https://www.infoworld.com/article/4066477/why-observability-needs-apache-iceberg.html#:~:text=Iceberg%20and%20OTel,the%20need%20to%20replace%20them.
