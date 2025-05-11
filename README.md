# Dispatch play

Small consumer and producer to play around with fan-outs of consumer work.

Expects a local kafka cluster to be running and exposed on `localhost:9021` (no auth).
Personaly, using a small dockerized single node Confluent cluster ([local-confluent](https://github.com/VinceDeslo/local-confluent)).

### Getting up and running

```bash
nixd
just fmt
just run
```

### Implementation plan
- [x] protobuf serde over a topic with an "Analytics" message structure.
- [ ] register schema via schema registry.
- [ ] spawn threads to handle analytics events in parallel (fan-out).
- [ ] load payload into arrow for stream processing.
- [ ] dispatch calculations to new topic (fan-in).
- [ ] sink into some form of end storage (Parquet in S3 if I'm not too lazy).

### Diagram

```mermaid
graph LR
    A[Producer] --> B[/Kafka Topic: events.analytics.v1/]
    B --> C{Consumer}
    C --> D[Arrow In-Mem DB]
    D --> E1(Processing 1)
    D --> E2(Processing 2)
    D --> E3(...)
    D --> E4(Processing N)
    E1 --> F[Producer]
    E2 --> F
    E3 --> F
    E4 --> F
    F --> G[/Kafka Topic: events.analytics.clean/]
```
