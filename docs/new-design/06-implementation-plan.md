# Implementation Plan

## Overview

This document outlines the implementation roadmap for the new repository service architecture.

## Prerequisites

- [x] Analyzed existing architecture (legacy repo service)
- [x] Reviewed platform-libraries proto definitions
- [x] Reviewed connector-intake-service patterns
- [x] Documented new architecture design
- [ ] Set up development environment
- [ ] Configure infrastructure (Redis, Kafka, S3, MySQL)

## Phase 1: Foundation (Weeks 1-2)

### 1.1 Infrastructure Setup

**Goal:** Set up development infrastructure

**Tasks:**
- [ ] Set up Docker Compose for local development
  - Redis (single instance for dev, cluster for prod)
  - Kafka + Zookeeper
  - Apicurio Schema Registry
  - MinIO (S3-compatible storage)
  - MySQL 8.0
- [ ] Configure Quarkus application
  - Add dependencies (Redis, Kafka, S3, MySQL)
  - Configure dev services
  - Set up application.properties
- [ ] Create database schema
  - Drives table
  - Nodes table
  - Upload tracking (optional, mainly Redis-based)

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  apicurio:
    image: apicurio/apicurio-registry-mem:latest
    ports:
      - "8080:8080"
    environment:
      REGISTRY_AUTH_ANONYMOUS_READ_ACCESS_ENABLED: "true"

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

  mysql:
    image: mysql:8.0
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: repository_db
      MYSQL_USER: repository
      MYSQL_PASSWORD: repository
    volumes:
      - mysql_data:/var/lib/mysql

volumes:
  redis_data:
  minio_data:
  mysql_data:
```

**Deliverables:**
- Working Docker Compose stack
- Quarkus application connects to all services
- Health checks pass for all services

**Estimated Time:** 3 days

### 1.2 Core Data Models

**Goal:** Define Java entities and repositories

**Tasks:**
- [ ] Create protobuf definitions (use platform-libraries as reference)
  - `upload_service.proto`
  - `repository_events.proto`
- [ ] Generate gRPC stubs
- [ ] Create JPA entities
  - `Drive` entity
  - `Node` entity
- [ ] Create Panache repositories
  - `DriveRepository`
  - `NodeRepository`
- [ ] Write unit tests for entities

**Example Entity:**
```java
@Entity
@Table(name = "nodes")
public class Node extends PanacheEntityBase {
    @Id
    @GeneratedValue
    public Long id;

    @Column(nullable = false, unique = true)
    public String documentId;  // UUID

    @ManyToOne
    public Drive drive;

    public String name;
    public String contentType;
    public Long sizeBytes;
    public String s3Key;
    public String s3Etag;

    @Column(columnDefinition = "JSON")
    public String metadata;

    @CreationTimestamp
    public LocalDateTime createdAt;

    @UpdateTimestamp
    public LocalDateTime updatedAt;
}
```

**Deliverables:**
- Proto definitions compiled
- JPA entities created
- Repositories with basic CRUD
- Unit tests passing

**Estimated Time:** 4 days

### 1.3 Redis Integration

**Goal:** Set up Redis client and data structures

**Tasks:**
- [ ] Configure Quarkus Redis client
- [ ] Create `UploadStateManager` service
  - Methods for state CRUD
  - Methods for chunk storage
  - Methods for ETag collection
- [ ] Create Redis pub/sub infrastructure
  - Channel definitions
  - Publisher abstraction
  - Subscriber abstraction
- [ ] Write integration tests with Testcontainers

**Example Service:**
```java
@ApplicationScoped
public class UploadStateManager {

    @Inject
    RedisClient redis;

    public void initializeUpload(String uploadId, String nodeId, String s3UploadId, String s3Key) {
        redis.hset(Arrays.asList("upload:" + uploadId + ":state",
            "node_id", nodeId,
            "s3_upload_id", s3UploadId,
            "s3_key", s3Key,
            "status", "PENDING",
            "created_at", String.valueOf(System.currentTimeMillis())
        ));
        redis.expire("upload:" + uploadId + ":state", 86400);
    }

    public void storeChunk(String uploadId, int chunkNumber, byte[] data) {
        String key = String.format("upload:%s:chunks:%d", uploadId, chunkNumber);
        redis.setex(key, 3600, data);
    }

    public void addETag(String uploadId, int chunkNumber, String etag) {
        redis.zadd("upload:" + uploadId + ":etags", chunkNumber, etag);
    }

    // ... more methods
}
```

**Deliverables:**
- Redis client configured
- `UploadStateManager` service implemented
- Pub/sub infrastructure ready
- Integration tests passing

**Estimated Time:** 3 days

**Total Phase 1:** 10 days (2 weeks)

## Phase 2: Upload Service Implementation (Weeks 3-5)

### 2.1 Phase 1 Upload (InitiateUpload)

**Goal:** Implement instant node_id generation

**Tasks:**
- [ ] Implement `InitiateUpload` gRPC endpoint
  - Validate request parameters
  - Generate node_id (or use client-provided)
  - Create MySQL node record (status=PENDING)
  - Initiate S3 multipart upload
  - Initialize Redis state
  - Return response immediately
- [ ] Handle small file optimization (< 100MB)
  - Direct PUT to S3
  - Mark node as ACTIVE immediately
  - Skip Redis state
- [ ] Add encryption support
  - SSE-S3 (default)
  - SSE-KMS (with key ARN)
  - SSE-C (client-provided key)
- [ ] Write unit and integration tests

**Implementation:**
```java
@ApplicationScoped
public class NodeUploadService implements NodeUploadGrpc.NodeUploadImplBase {

    @Inject
    S3Client s3Client;

    @Inject
    UploadStateManager stateManager;

    @Inject
    NodeRepository nodeRepository;

    @Override
    public void initiateUpload(InitiateUploadRequest request,
                               StreamObserver<InitiateUploadResponse> responseObserver) {
        String nodeId = generateNodeId();

        // Check if small file (direct upload)
        if (request.hasPayload() && request.getPayload().size() < 100 * 1024 * 1024) {
            handleSmallFileUpload(nodeId, request, responseObserver);
            return;
        }

        // Create S3 multipart upload
        String s3Key = buildS3Key(request.getConnectorId(), nodeId);
        String s3UploadId = initiateS3MultipartUpload(s3Key, request);

        // Initialize Redis state
        stateManager.initializeUpload(nodeId, nodeId, s3UploadId, s3Key);

        // Create node record in database
        Node node = new Node();
        node.documentId = nodeId;
        node.name = request.getName();
        node.status = "UPLOADING";
        node.s3Key = s3Key;
        nodeRepository.persist(node);

        // Return response immediately
        InitiateUploadResponse response = InitiateUploadResponse.newBuilder()
            .setNodeId(nodeId)
            .setUploadId(nodeId)
            .setState(UploadState.PENDING)
            .setCreatedAtEpochMs(System.currentTimeMillis())
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private String initiateS3MultipartUpload(String s3Key, InitiateUploadRequest request) {
        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(s3Key)
            .contentType(request.getMimeType());

        // Handle encryption
        if (request.hasEncryption()) {
            applyEncryption(builder, request.getEncryption());
        }

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(builder.build());
        return response.uploadId();
    }
}
```

**Deliverables:**
- `InitiateUpload` endpoint working
- Small file optimization implemented
- Encryption support (all 3 types)
- Tests passing

**Estimated Time:** 5 days

### 2.2 Phase 2 Upload (UploadChunk)

**Goal:** Implement parallel chunk upload

**Tasks:**
- [ ] Implement `UploadChunk` gRPC endpoint (unary)
  - Validate upload_id exists
  - Store chunk in Redis
  - Update received_chunks counter
  - Publish chunk_received event
  - Return acknowledgment immediately
- [ ] Handle `is_last` flag
  - Set total_chunks when last chunk received
- [ ] Add idempotency checks
  - Prevent duplicate chunk storage
- [ ] Write tests (including parallel upload test)

**Implementation:**
```java
@Override
public void uploadChunk(UploadChunkRequest request,
                       StreamObserver<UploadChunkResponse> responseObserver) {
    String uploadId = request.getUploadId();
    int chunkNumber = request.getChunkNumber();
    byte[] data = request.getData().toByteArray();

    // Store chunk in Redis
    stateManager.storeChunk(uploadId, chunkNumber, data);

    // Update state
    long receivedChunks = stateManager.incrementReceivedChunks(uploadId);

    // Handle last chunk
    if (request.getIsLast()) {
        int totalChunks = chunkNumber + 1;
        stateManager.setTotalChunks(uploadId, totalChunks);
        stateManager.setLastReceived(uploadId, true);
    }

    // Publish notification for workers
    publisher.publish("upload:chunk:received", Json.createObjectBuilder()
        .add("upload_id", uploadId)
        .add("chunk_number", chunkNumber)
        .add("is_last", request.getIsLast())
        .build().toString());

    // Return acknowledgment
    UploadChunkResponse response = UploadChunkResponse.newBuilder()
        .setNodeId(request.getNodeId())
        .setState(UploadState.UPLOADING)
        .setBytesUploaded(receivedChunks * data.length)  // Approximate
        .setChunkNumber(chunkNumber)
        .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
}
```

**Deliverables:**
- `UploadChunk` endpoint working
- Idempotency handling
- Parallel upload test passing
- Redis pub/sub working

**Estimated Time:** 4 days

### 2.3 Background Workers

**Goal:** Process chunks and upload to S3

**Tasks:**
- [ ] Create `ChunkProcessorWorker` service
  - Subscribe to `upload:chunk:received` channel
  - Claim chunks from Redis (atomic GETDEL)
  - Upload to S3 as multipart
  - Store ETags in Redis
  - Handle encryption for SSE-C
- [ ] Create `UploadCompletionWorker` service
  - Check when all chunks uploaded
  - Complete S3 multipart upload
  - Update MySQL node record
  - Publish Kafka completion event
  - Clean up Redis state
- [ ] Add retry logic with exponential backoff
- [ ] Add monitoring and metrics
- [ ] Write integration tests

**Implementation:**
```java
@ApplicationScoped
public class ChunkProcessorWorker {

    @Inject
    S3Client s3Client;

    @Inject
    UploadStateManager stateManager;

    @Inject
    RedisClient redis;

    @Incoming("upload-chunk-received")
    public void processChunk(String message) {
        JsonObject json = Json.createReader(new StringReader(message)).readObject();
        String uploadId = json.getString("upload_id");
        int chunkNumber = json.getInt("chunk_number");

        // Claim chunk (atomic GETDEL)
        byte[] chunkData = stateManager.claimChunk(uploadId, chunkNumber);

        if (chunkData == null) {
            // Already processed by another worker
            return;
        }

        try {
            // Get upload metadata
            Map<String, String> state = stateManager.getState(uploadId);
            String s3UploadId = state.get("s3_upload_id");
            String s3Key = state.get("s3_key");

            // Upload part to S3 (1-based part numbers)
            int partNumber = chunkNumber + 1;
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .uploadId(s3UploadId)
                .partNumber(partNumber)
                .contentLength((long) chunkData.length)
                .build();

            UploadPartResponse response = s3Client.uploadPart(
                uploadPartRequest,
                RequestBody.fromBytes(chunkData)
            );

            String etag = response.eTag();

            // Store ETag in Redis
            stateManager.addETag(uploadId, chunkNumber, etag);

            // Update progress
            stateManager.incrementUploadedChunks(uploadId);

            logger.info("Uploaded part {} for upload {}, ETag: {}", partNumber, uploadId, etag);

            // Check if upload complete
            checkAndCompleteUpload(uploadId);

        } catch (Exception e) {
            logger.error("Failed to upload chunk {} for upload {}", chunkNumber, uploadId, e);
            retryChunkUpload(uploadId, chunkNumber, chunkData);
        }
    }

    private void checkAndCompleteUpload(String uploadId) {
        Map<String, String> state = stateManager.getState(uploadId);

        if (!"true".equals(state.get("is_last_received"))) {
            return;  // Don't know total chunks yet
        }

        int totalChunks = Integer.parseInt(state.get("total_chunks"));
        int uploadedChunks = Integer.parseInt(state.get("uploaded_chunks"));

        if (uploadedChunks == totalChunks) {
            // Trigger completion
            completionWorker.completeUpload(uploadId);
        }
    }
}
```

**Deliverables:**
- `ChunkProcessorWorker` implemented
- `UploadCompletionWorker` implemented
- S3 multipart completion working
- End-to-end upload test passing

**Estimated Time:** 6 days

**Total Phase 2:** 15 days (3 weeks)

## Phase 3: Status & Management (Week 6)

### 3.1 Progress Tracking

**Tasks:**
- [ ] Implement `GetUploadStatus` endpoint (polling)
- [ ] Implement `StreamUploadProgress` endpoint (streaming)
- [ ] Add progress publishing to workers
- [ ] Write tests

**Estimated Time:** 3 days

### 3.2 Upload Management

**Tasks:**
- [ ] Implement `CancelUpload` endpoint
  - Abort S3 multipart upload
  - Clean up Redis state
  - Update database
- [ ] Implement cleanup job for abandoned uploads
  - Scan for stale uploads (> 24 hours)
  - Abort and clean up
- [ ] Write tests

**Estimated Time:** 2 days

**Total Phase 3:** 5 days (1 week)

## Phase 4: Kafka Integration (Week 7)

### 4.1 Event Publishing

**Tasks:**
- [ ] Set up Kafka producer with Apicurio
- [ ] Implement event publishers
  - `UploadInitiated`
  - `UploadCompleted`
  - `UploadFailed`
  - `DocumentCreated`
  - `DocumentUpdated`
  - `DocumentDeleted`
- [ ] Add event publishing to service methods
- [ ] Write integration tests

**Estimated Time:** 4 days

### 4.2 Event Consumers (Self-Consumption)

**Tasks:**
- [ ] Implement cache invalidation consumer
  - Subscribe to document update/delete events
  - Invalidate Redis cache entries
- [ ] Write tests

**Estimated Time:** 1 day

**Total Phase 4:** 5 days (1 week)

## Phase 5: Testing & Optimization (Weeks 8-9)

### 5.1 Integration Testing

**Tasks:**
- [ ] End-to-end upload tests
  - Small files (< 100MB)
  - Large files (> 1GB)
  - Very large files (> 10GB)
- [ ] Parallel upload tests (10+ concurrent uploads)
- [ ] Failure scenario tests
  - Worker crashes mid-upload
  - S3 temporary unavailability
  - Redis temporary unavailability
  - Client disconnect
- [ ] Encryption tests (SSE-S3, SSE-KMS, SSE-C)

**Estimated Time:** 5 days

### 5.2 Performance Testing

**Tasks:**
- [ ] Benchmark single file upload
- [ ] Benchmark concurrent uploads (10, 50, 100)
- [ ] Identify bottlenecks
- [ ] Optimize hot paths
- [ ] Load test with realistic workload

**Estimated Time:** 3 days

### 5.3 Monitoring & Observability

**Tasks:**
- [ ] Add Prometheus metrics
  - Upload latency
  - Chunk processing time
  - S3 upload time
  - Error rates
- [ ] Add structured logging
- [ ] Create Grafana dashboards
- [ ] Set up alerts

**Estimated Time:** 2 days

**Total Phase 5:** 10 days (2 weeks)

## Phase 6: Documentation & Deployment (Week 10)

### 6.1 Documentation

**Tasks:**
- [ ] API documentation (OpenAPI/gRPC docs)
- [ ] Deployment guide
- [ ] Operations runbook
- [ ] Client library examples
- [ ] Migration guide from legacy

**Estimated Time:** 3 days

### 6.2 Deployment Preparation

**Tasks:**
- [ ] Containerize application (Docker)
- [ ] Create Kubernetes manifests
- [ ] Set up CI/CD pipeline
- [ ] Configure production Redis cluster
- [ ] Configure production Kafka cluster
- [ ] Set up production monitoring

**Estimated Time:** 2 days

**Total Phase 6:** 5 days (1 week)

## Total Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Phase 1: Foundation | 2 weeks | 2 weeks |
| Phase 2: Upload Service | 3 weeks | 5 weeks |
| Phase 3: Status & Management | 1 week | 6 weeks |
| Phase 4: Kafka Integration | 1 week | 7 weeks |
| Phase 5: Testing & Optimization | 2 weeks | 9 weeks |
| Phase 6: Documentation & Deployment | 1 week | 10 weeks |

**Total: 10 weeks (~2.5 months)**

## Team Composition

**Recommended team:**
- 1 Senior Backend Developer (Quarkus, gRPC, Redis)
- 1 Mid-level Backend Developer (S3, Kafka, MySQL)
- 1 DevOps Engineer (Docker, Kubernetes, monitoring)
- 1 QA Engineer (testing, automation)

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| **Redis memory exhaustion** | Configure maxmemory policy, monitor memory usage, use separate Redis for chunks vs state |
| **S3 rate limits** | Implement exponential backoff, use S3 Transfer Acceleration for global uploads |
| **Kafka schema evolution** | Use Apicurio compatibility checks, test schema changes before deployment |
| **Worker crashes** | Multiple worker instances, Redis state survives crashes, automatic recovery |
| **Database migration issues** | Test migration on staging first, have rollback plan, gradual migration |

## Success Criteria

- [ ] All unit tests passing (>90% coverage)
- [ ] All integration tests passing
- [ ] Load test passes (100 concurrent 1GB uploads in < 30 minutes)
- [ ] Zero data loss in failure scenarios
- [ ] < 100ms response time for InitiateUpload
- [ ] < 20 seconds for 1GB file upload (on gigabit network)
- [ ] Horizontal scaling verified (3 instances handle 3x load)
- [ ] Monitoring dashboards operational
- [ ] Documentation complete

## Post-Launch

### Month 1: Monitoring & Tuning
- Monitor production metrics
- Tune Redis/Kafka/S3 settings
- Fix bugs, performance issues
- Collect user feedback

### Month 2-3: Feature Additions
- Add download service (symmetric with upload)
- Add batch upload endpoints
- Add upload resume (for network interruptions)
- Add compression support

### Month 4-6: Legacy Migration
- Migrate existing connectors
- Deprecate legacy service
- Archive legacy code

## Next Steps

1. **Review this plan** with team
2. **Set up development environment** (Phase 1.1)
3. **Create project in issue tracker** with tasks
4. **Schedule kickoff meeting**
5. **Begin implementation** following this plan
