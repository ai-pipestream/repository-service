## repository-service

`repository-service` is the platform’s **durable repository** (“librarian”) for documents and repository metadata.

It provides:
- **Durable storage**: bytes in S3-compatible object storage; metadata + versioning in the database.
- **Repository metadata APIs**: drives, nodes, paths, and browsing/search support.
- **Event emission**: Kafka events on state changes so other services (e.g., `opensearch-manager`) can build indexes without sharing databases.

For the full architecture and decisions, see [`DESIGN.md`](./DESIGN.md).

### Key concepts
- **PipeDoc**: the primary pipeline document container.
- **Claim-check pattern**: large payloads stored externally; protobuf carries a reference.
- **Hydration/dehydration**: control when binary blobs are inline vs referenced.

### Protobufs (source of record)
All gRPC APIs and message contracts are defined in `pipestream-protos`.

Relevant proto areas:
- `ai.pipestream.repository.*` (repo services)
- `ai.pipestream.data.v1.PipeDoc` and related core types

### Build & test

```bash
/home/krickert/IdeaProjects/repository-service/gradlew test
```

### Run (dev)

```bash
/home/krickert/IdeaProjects/repository-service/gradlew quarkusDev
```

### Current status
- The repository-service is under active development.
- The design direction is documented in [`DESIGN.md`](./DESIGN.md).

### Notes
- This repo uses the proto toolchain for code generation (instead of Quarkus gRPC codegen).
- `docs/new-design/*` contains historical design notes and phase documents; `DESIGN.md` is the current consolidated source of truth.
