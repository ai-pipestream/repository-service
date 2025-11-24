# Repository Service Refactoring - Current Work Summary

This document summarizes the active refactoring effort for the `core-services/repository-service` (the new, non-legacy implementation).

## Goal
Transition `repository-service` to a fully **event-driven, reactive architecture** using the "Log-Structured Data Flow" principles.

## Architectural Changes

### 1. "Kafka-First" Persistence
*   **Old Way (Legacy):** `gRPC Request` → `Blocking DB Write` → `Blocking S3 Upload` → `Kafka Event` → `Response`.
*   **New Way:** `gRPC Request` → **`S3 Transfer Manager (Reactive)`** → **`Emit Kafka Event`** → `Response`.
*   **Metadata:** The actual database persistence (saving `Node` entities to MySQL) is decoupled and handled asynchronously by a Kafka Consumer listening to the events we emitted.

### 2. Reactive S3 Operations
*   Replaced manual multipart upload logic with **AWS SDK v2 S3 Transfer Manager**.
*   All S3 operations are now wrapped in Mutiny `Uni` types for non-blocking execution.
*   **Key Components:**
    *   `ai.pipestream.repository.s3.S3TransferManagerProducer`: CDI producer configuring the transfer manager.
    *   `ai.pipestream.repository.service.DocumentStorageService`: Service encapsulating S3 upload/download logic using the Transfer Manager.

### 3. Kafka "Gold Standard" Compliance
All Kafka integration strictly follows the platform standards defined in `ai-pipestream-homepage/docs/developer/kafka-apicurio-guide.md`:
*   **Key:** `java.util.UUID` (Deterministic based on ID).
*   **Value:** Protobuf message (e.g., `RepositoryEvent`, `DocumentEvent`).
*   **Configuration:** Rely on `PipelineKafkaConfigSource` defaults (in `pipeline-commons`) for serializers/deserializers.
*   **Testing:** strictly `docker-compose` based (no in-memory).

### 4. Search & Indexing Flow (Admin UI Support)
Distinct from the AI Pipeline, this flow powers the "File Browser" in the Admin UI.
1.  **Ingestion:** `repository-service` uploads payload to S3 → Emits `RepositoryEvent.Created` (Kafka).
2.  **Persistence:** `repository-service` (Metadata Indexer) consumes `Created` → Persists metadata to MySQL.
3.  **Confirmation:** Upon successful DB commit, Metadata Indexer emits a confirmation event (e.g., `RepositoryEvent.MetadataPersisted` or similar state).
4.  **System Indexing:** `opensearch-manager` consumes this confirmation event → Indexes file metadata (Name, Path, Size, Tags) into the `system-files` OpenSearch index.
5.  **Usage:** Admin UI calls `repository-service` search endpoints, which proxy queries to `opensearch-manager`.

## Current Status of `core-services/repository-service`

### Implemented
*   [x] `build.gradle`: Added `s3-transfer-manager` dependency.
*   [x] `ai.pipestream.repository.s3.S3TransferManagerProducer`: Created.
*   [x] `ai.pipestream.repository.service.DocumentStorageService`: Implemented with `uploadPipeDoc` and `downloadPipeDoc` using `S3TransferManager`.

### Pending Tasks
1.  **Implement `RepositoryGrpcService`**:
    *   Update `savePipeDoc` (or equivalent) to call `DocumentStorageService.uploadPipeDoc`.
    *   Inject `MutinyEmitter<Record<UUID, RepositoryEvent>>` to emit events upon success.
2.  **Create Metadata Indexer (Consumer)**:
    *   Create a Kafka Consumer service that listens to `repository-events`.
    *   Implement the logic to persist `Node` entities to MySQL when `Created` events are received.
    *   **Crucial:** Ensure it emits the "Metadata Complete" event upon success to trigger OpenSearch indexing.
3.  **Cleanup**: Remove empty/unused classes (`NodeUploadServiceImpl.java` if it's a vestige).

## Reference Documents
*   **Graph Architecture:** `dev-assets/docs/research/network-graph-architecture-summary.md`
*   **Log-Structured Theory:** `ai-pipestream-homepage/docs/research/log-structured-data-flow-whitepaper.md`
*   **Kafka Guidelines:** `ai-pipestream-homepage/docs/developer/kafka-apicurio-guide.md`