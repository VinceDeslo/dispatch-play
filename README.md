# Dispatch play

Small consumer and producer to play around with fan-outs of consumer work.

Expects a local kafka cluster to be running and exposed on `localhost:9021` (no auth).

### Getting up and running

```bash
nixd
just fmt
just run
```

### Ideas to implement
- [ ] protobuf serde over the topic with schema registry.
- [ ] load payload into arrow for stream processing.
- [ ] dispatch to new topic (fan-in).
- [ ] sink into some form of end storage.
