# Repository Service - New Architecture Design

This folder contains the comprehensive design documentation for the new repository service architecture.

## Background

The repository service has evolved through two iterations, each addressing different concerns:

1. **repository-service-old**: Original Quarkus-based implementation with gRPC services for filesystem operations
2. **repo-service**: Attempted redesign that became overly complex

This design represents a **third iteration** that simplifies the input interface while maintaining scalability and reliability.

## Key Design Changes

### From Complex to Simple

**Old approach (problems):**
- Overly complex input interface
- Streaming APIs with backpressure complexity
- All-or-nothing upload semantics
- State lost on service restart

**New approach (solutions):**
- **Intake-first pattern**: First call returns metadata immediately
- **Two-phase upload**: Separate metadata from chunk uploads
- **Parallel chunk processing**: Unary calls enable true parallelism
- **Redis-backed state**: Survives restarts, enables horizontal scaling

## Architecture Documents

Read these documents in order:

### 1. [Overview](00-overview.md)
**Start here!** High-level architecture overview covering:
- Intake-first pattern
- Two-phase upload process
- Redis coordination layer
- S3 multipart upload coordination
- Event-driven architecture with Kafka
- Technology stack

### 2. [Upload Flow](01-upload-flow.md)
Detailed upload flow documentation:
- Phase 1: InitiateUpload (instant node_id)
- Phase 2: Parallel chunk upload
- Progress tracking (polling and streaming)
- Error handling and recovery
- Performance characteristics

### 3. [Redis Queuing](02-redis-queuing.md)
Redis data structures and queuing patterns:
- Upload state tracking
- Chunk storage with TTL
- ETag collection (sorted sets)
- Pub/sub coordination
- Memory management
- Failure scenarios

### 4. [S3 Multipart Upload](03-s3-multipart.md)
S3 multipart upload coordination:
- Three-phase S3 process
- Chunk size strategy
- Part upload and completion
- Encryption support (SSE-S3, SSE-KMS, SSE-C)
- Error handling and retry
- Performance optimization

### 5. [Kafka Events](04-kafka-events.md)
Kafka event patterns and integration:
- Lean event design (state changes only)
- Deterministic event IDs
- Event topics and schemas
- Callback pattern for consumers
- Apicurio schema registry integration
- Producer and consumer examples

### 6. [Architecture Comparison](05-comparison.md)
Detailed comparison with legacy architecture:
- Component-by-component comparison
- Performance benchmarks
- Migration path
- Scalability improvements
- Benefits and trade-offs

### 7. [Implementation Plan](06-implementation-plan.md)
10-week implementation roadmap:
- Phase 1: Foundation (infrastructure setup)
- Phase 2: Upload service implementation
- Phase 3: Status & management
- Phase 4: Kafka integration
- Phase 5: Testing & optimization
- Phase 6: Documentation & deployment

## Quick Start

### For Architects & Tech Leads

1. Read [00-overview.md](00-overview.md) - 15 minutes
2. Skim [05-comparison.md](05-comparison.md) - 10 minutes
3. Review [06-implementation-plan.md](06-implementation-plan.md) - 15 minutes

**Total: 40 minutes** for high-level understanding and timeline.

### For Backend Engineers

1. Read [00-overview.md](00-overview.md) - 15 minutes
2. Deep dive [01-upload-flow.md](01-upload-flow.md) - 30 minutes
3. Study [02-redis-queuing.md](02-redis-queuing.md) - 30 minutes
4. Study [03-s3-multipart.md](03-s3-multipart.md) - 30 minutes

**Total: 105 minutes** for implementation-ready knowledge.

### For DevOps Engineers

1. Read [00-overview.md](00-overview.md) - 15 minutes
2. Review infrastructure in [06-implementation-plan.md](06-implementation-plan.md) Phase 1.1 - 20 minutes
3. Study monitoring in [06-implementation-plan.md](06-implementation-plan.md) Phase 5.3 - 15 minutes

**Total: 50 minutes** for deployment readiness.

## Key Concepts

### Intake-First Pattern

The service follows an **intake-first pattern** inspired by connector-intake-service:

1. **First call (blocking)**: Client sends file metadata → Server returns node_id and upload_id immediately
2. **Subsequent calls (non-blocking)**: Client sends chunks in parallel using the upload_id

This pattern enables:
- Instant node_id generation (no waiting for upload)
- True parallel chunk uploads
- Simplified client code
- Better scalability

### Two-Phase Upload

```
Phase 1 (Initiate Upload)
Client → Server: File metadata
Server → Client: node_id, upload_id (~100ms)

Phase 2 (Upload Chunks)
Client → Server: Chunk 0, Chunk 1, Chunk 2... (parallel)
Server → Client: ACK, ACK, ACK... (immediate)
Background: Server → S3 (async workers)
```

### Redis as Coordination Layer

Redis serves multiple roles:
- **Chunk queue**: Temporary storage before S3 upload
- **State tracking**: Upload progress and status
- **ETag collection**: Ordered list for S3 multipart completion
- **Worker coordination**: Pub/sub for async processing

### S3 Multipart Upload

For large files (> 100MB), we use S3 multipart uploads:
- **Phase 1**: Initiate multipart upload → get s3_upload_id
- **Phase 2**: Upload chunks as parts (parallel) → collect ETags
- **Background**: When all ETags collected → complete multipart upload

Benefits:
- Support files up to 5TB
- Parallel part uploads
- Retry individual parts (not entire file)

### Kafka Event Streaming

Events are published at key points:
- `repository.uploads.initiated` - Upload started
- `repository.uploads.completed` - Upload finished
- `repository.documents.created` - Document created
- `repository.documents.updated` - Document updated
- `repository.documents.deleted` - Document deleted

Consumers use **callback pattern**: Event triggers consumer to call back to repository service for latest data.

## Technology Stack

- **Framework**: Quarkus (Java 21)
- **Communication**: gRPC (defined in platform-libraries)
- **Storage**:
  - S3/MinIO (persistent object storage)
  - MySQL 8.0 (metadata)
- **Caching/Queue**: Redis 7
- **Messaging**: Kafka + Apicurio Schema Registry
- **Monitoring**: Prometheus + Grafana

## Design Principles

1. **Fast Response**: Return node_id immediately, don't wait for upload
2. **Resilient**: Redis state survives service restarts
3. **Scalable**: Horizontally scalable (multiple instances share Redis)
4. **Simple**: Unary calls easier than streaming
5. **Secure**: Support for client-provided encryption (SSE-C)
6. **Observable**: Full metrics and logging
7. **Event-Driven**: Kafka events enable loose coupling

## Performance Goals

| Metric | Target | vs Legacy |
|--------|--------|-----------|
| Time to node_id | < 100ms | 600x faster |
| 1GB upload time | < 20 seconds | 4x faster |
| Memory per upload | < 100MB | 10x less |
| Concurrent uploads | 100+ | 10x more |
| Recovery time | < 5 minutes | 60x faster |

## Security Considerations

### Encryption at Rest

All data in S3 is encrypted using one of:
- **SSE-S3**: S3-managed keys (default)
- **SSE-KMS**: AWS KMS-managed keys
- **SSE-C**: Client-provided keys (maximum security)

### Encryption in Transit

- gRPC with TLS
- Redis with TLS (production)
- Kafka with SASL/SSL (production)

### Key Management

For SSE-C (client-provided keys):
- Keys never logged or persisted in plaintext
- Stored temporarily in Redis with short TTL
- Automatically deleted after upload completes
- Future: Integrate with HashiCorp Vault or AWS Secrets Manager

## Monitoring & Observability

### Metrics (Prometheus)

- Upload latency (p50, p95, p99)
- Chunk processing time
- S3 upload time
- Error rates by type
- Queue depth (chunks pending)
- Throughput (bytes/sec)

### Dashboards (Grafana)

- Upload overview (success rate, latency)
- System health (Redis, Kafka, S3, MySQL)
- Worker performance (queue depth, processing time)
- Error analysis (error types, affected uploads)

### Alerts

- Upload failure rate > 5%
- Redis memory > 80%
- Kafka lag > 1000 messages
- S3 error rate > 1%
- Worker queue depth > 1000

## FAQ

### Q: Why not use gRPC streaming for chunks?

**A:** Unary calls enable true client-side parallelism. With streaming, client must manage a single stream with backpressure, limiting parallelism. Unary calls let clients fire 10+ concurrent requests easily.

### Q: Why Redis instead of Kafka for chunk queue?

**A:** Redis is better for:
- Random access (workers claim specific chunks)
- Fast writes (< 1ms vs Kafka ~10ms)
- TTL (auto-cleanup of stale chunks)
- Sorted sets (ordered ETag collection)

Kafka is better for event streaming (which we use for state changes).

### Q: What happens if Redis fails?

**A:** Active uploads will fail, but can be retried. Redis should be:
- Clustered for high availability
- Persisted (AOF or RDB)
- Monitored with alerts

For critical deployments, consider Redis Sentinel or Redis Cluster.

### Q: How do we handle very large files (> 100GB)?

**A:** S3 multipart upload supports up to 5TB. For even larger files:
- Use larger chunks (up to 5GB per chunk)
- Increase Redis memory for chunk storage
- Consider streaming chunks directly to S3 (skip Redis storage)

### Q: Can we resume interrupted uploads?

**A:** Partial support:
- Redis state tracks received chunks
- Client can retry missing chunks
- Full resume feature planned for future release

## Related Projects

This design builds on patterns from:

- **[platform-libraries](https://github.com/ai-pipestream/platform-libraries)**: Proto definitions, shared utilities
- **[connector-intake-service](https://github.com/ai-pipestream/connector-intake-service)**: Intake-first pattern, session management
- **[platform-registration-service](https://github.com/ai-pipestream/platform-registration-service)**: Service discovery
- **[Apache Tika](https://github.com/apache/tika)**: Content extraction (downstream consumer)

## Contributing

When updating this design:

1. **Keep documents in sync**: Changes to architecture should be reflected across all relevant docs
2. **Update implementation plan**: Adjust timelines and tasks as needed
3. **Add examples**: Code snippets help clarify concepts
4. **Test your examples**: Ensure code examples actually work
5. **Update this README**: Keep the overview current

## License

See repository root for license information.

## Contact

For questions or feedback on this design:
- Open an issue in the repository
- Contact the architecture team
- Join #repository-service on Slack

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Status:** Design Phase
**Next Review:** Before Phase 1 kickoff
