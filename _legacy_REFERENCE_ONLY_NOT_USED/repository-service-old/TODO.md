# Repository Service TODO

## Current Status (August 11, 2025)

### ✅ Completed Components

#### 1. Core Infrastructure
- [x] **Database Entities**: `RepositoryDrive` and `RepositoryNode` with full JPA mapping
  - Full Panache entity implementation with reactive repositories
  - Soft-delete pattern implemented with `@SQLDelete` and `@Where` annotations
  - Proper relationships: `@OneToMany` and `@ManyToOne` with cascade operations
  - JSONB metadata fields for flexible document storage
  
- [x] **Persistence Layer**: Reactive Panache repositories with proper transaction handling
  - `DriveRepository` and `NodeRepository` with custom queries
  - Transaction boundaries properly managed with `@WithTransaction`
  - Optimistic locking with version fields
  
- [x] **S3 Integration**: MinIO/S3 backend for actual file storage
  - Binary payload storage separated from metadata
  - Configurable bucket and prefix strategies
  - Support for both MinIO (dev) and AWS S3 (prod)
  
- [x] **gRPC Services**: Full CRUD operations for filesystem, drives, and nodes
  - Complete FilesystemService implementation
  - DriveService with all drive management operations  
  - NodeService with hierarchy management
  
- [x] **Event Notifications**: Kafka producers for all entity changes
  - Drive, Node, PipeDoc, Module, ProcessRequest, ProcessResponse events
  - Using MutinyEmitter for non-blocking event publishing
  - Protobuf serialization with Apicurio Registry
  
- [x] **Dynamic gRPC Client Management**: Fixed channel leak issues
  - Implemented Caffeine-based cache with automatic eviction (15 min TTL)
  - Proper channel shutdown on eviction and @PreDestroy
  - Configurable per environment (dev/test/prod)
  - Prevents ManagedChannelOrphanWrapper warnings

#### 2. Services Implemented
- [x] **DriveService**: Complete CRUD for repository drives
- [x] **NodeService**: Full filesystem operations including hierarchy management  
- [x] **FilesystemService**: High-level filesystem operations combining drives and nodes
- [x] **GraphService & ModuleService**: Cache management and service coordination
- [x] **NotificationEmitters**: All Kafka event publishers using MutinyEmitter

#### 3. Testing & Configuration
- [x] **Test Infrastructure**: Comprehensive test suite with Testcontainers
- [x] **Configuration Management**: Unified application.properties with environment profiles
  - All test configurations moved to main file with %test prefix
  - Dynamic gRPC channel settings added
  - Test noise reduction (scheduler disabled, log levels adjusted)
- [x] **Health Checks**: Readiness and liveness probes configured
- [x] **Service Registration**: Consul integration for service discovery

### 🔄 Current Test Status

**16 Failing Tests** in MinioFilesystemServiceImpl (to be addressed after DB/search integration):
- DriveManagementTest: 3 failures
- FilesystemAdvancedTest: 2 failures  
- FilesystemEdgeCaseTest: 2 failures
- FilesystemProtobufTest: 1 failure
- FilesystemServiceTest: 6 failures
- FilesystemTypeValidationTest: 2 failures

These failures are expected as we're transitioning from pure S3 to PostgreSQL+S3 hybrid storage.

### 📋 Remaining Tasks

## Phase 1: Database Migration to MySQL (Immediate Priority)

### Rationale
Other platform components use MySQL. Switching from PostgreSQL ensures consistency and simplifies operations.

### Migration Tasks
- [ ] **Update Docker Compose**:
  ```yaml
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: pipeline_db
      MYSQL_USER: pipeline
      MYSQL_PASSWORD: pipeline
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
  ```

- [ ] **Update Dependencies** in `build.gradle`:
  ```gradle
  // Remove: implementation 'io.quarkus:quarkus-jdbc-postgresql'
  // Add: implementation 'io.quarkus:quarkus-jdbc-mysql'
  ```

- [ ] **Update Configuration**:
  ```properties
  quarkus.datasource.db-kind=mysql
  quarkus.datasource.jdbc.url=jdbc:mysql://localhost:3306/pipeline_db
  ```

- [ ] **Entity Adjustments**:
  - Change JSONB columns to JSON
  - UUID handling remains automatic with Hibernate
  - Review any custom native queries

- [ ] **Test Migration**:
  - Verify all entities persist correctly
  - Test JSON field operations
  - Validate transaction handling

## Phase 2: Complete Database Integration

### Service Layer Refactoring
- [ ] **Update FilesystemServiceImpl**:
  - Use DriveRepository for drive metadata
  - Use NodeRepository for node metadata
  - S3Repository only for binary payloads
  
- [ ] **Fix Failing Tests**:
  - Update test fixtures for database-backed operations
  - Mock database repositories where appropriate
  - Ensure S3 operations are properly isolated

### Transaction Management
- [ ] Ensure proper transaction boundaries
- [ ] Implement compensation for S3 operations if DB fails
- [ ] Add retry logic for transient failures

## Phase 3: OpenSearch Integration

### Event-Driven Indexing Architecture

#### Design Decision: Callback Pattern
We'll use an event-driven architecture where:
1. **Database Write** → Entity saved to MySQL
2. **Publish Event** → Kafka event with entity ID (not full payload)
3. **OpenSearch Consumer** → Receives event
4. **Fetch Fresh Data** → Consumer calls back to Repository Service via gRPC
5. **Index Document** → OpenSearch indexes the fetched data

This ensures:
- Data consistency (always index committed state)
- Handles rapid updates (final fetch gets latest version)
- Loose coupling (DB doesn't know search schema)
- Resilience (can replay events if indexing fails)

### Implementation Plan

#### 1. Define Indexing Events
```protobuf
message RepositoryIndexingEvent {
  string entity_type = 1;  // "drive", "node", "pipedoc"
  string entity_id = 2;
  string action = 3;       // "CREATE", "UPDATE", "DELETE"
  int64 version = 4;       // For optimistic concurrency
  google.protobuf.Timestamp timestamp = 5;
}
```

#### 2. Create OpenSearch Consumer Service
```java
@ApplicationScoped
public class SearchIndexingConsumer {
    @Inject FilesystemServiceGrpc.FilesystemServiceStub fsService;
    @Inject OpenSearchClient searchClient;
    
    @Incoming("indexing-events")
    public Uni<Void> onIndexingEvent(RepositoryIndexingEvent event) {
        return switch(event.getAction()) {
            case "DELETE" -> searchClient.delete(event.getEntityId());
            default -> fetchAndIndex(event);
        };
    }
    
    private Uni<Void> fetchAndIndex(RepositoryIndexingEvent event) {
        // Fetch fresh data via gRPC
        return fsService.getNode(NodeRequest.newBuilder()
                .setNodeId(event.getEntityId())
                .build())
            .onItem().transformToUni(node -> 
                searchClient.index(transformToDocument(node))
            );
    }
}
```

#### 3. Search API Implementation
- [ ] **Full-text search**: Search across all document content
- [ ] **Metadata search**: Query by custom metadata fields  
- [ ] **Hierarchical search**: Search within specific drives/folders
- [ ] **Faceted search**: Filter by type, date, size, tags
- [ ] **Similar document search**: Find related documents

#### 4. Bulk Operations
- [ ] Batch indexing for initial data load
- [ ] Re-indexing endpoint for schema changes
- [ ] Bulk update capabilities

## Phase 4: Performance & Reliability

### Caching Strategy
- [ ] Implement read-through cache for frequently accessed nodes
- [ ] Cache invalidation on updates
- [ ] Consider Redis for distributed caching (probably too expensive for this use case?)

### Circuit Breakers & Resilience
- [ ] Circuit breaker for OpenSearch availability
- [ ] Retry logic with exponential backoff
- [ ] Fallback to database search if OpenSearch unavailable (this is certainly overkill)

### Monitoring & Metrics
- [ ] Track indexing lag
- [ ] Monitor search latency
- [ ] Alert on indexing failures

## Phase 5: Advanced Features (Future)

### Version Control
- [ ] Track document versions in database
- [ ] Store diffs in S3
- [ ] Version comparison API
- [ ] Rollback capabilities

### Access Control
- [ ] Document-level permissions
- [ ] Audit logging
- [ ] Integration with auth service
- [ ] Encryption for sensitive metadata

### Analytics
- [ ] Usage statistics
- [ ] Search query patterns
- [ ] Access reports
- [ ] Quota management

## Success Metrics

### Performance Targets
- Search latency < 100ms (95th percentile)
- Indexing lag < 5 seconds
- 99.9% availability for reads
- Zero data loss for writes
- Support 10,000+ concurrent users

### Quality Gates
- All tests passing after MySQL migration
- No memory leaks (verified via profiling)
- Graceful degradation when OpenSearch unavailable
- Complete audit trail for all operations

## Technical Debt & Cleanup

### Code Cleanup
- [x] Consolidated test configurations into main application.properties
- [x] Fixed gRPC channel leaks with Caffeine cache
- [ ] Remove deprecated S3-only code paths
- [ ] Standardize error handling across services

### Documentation
- [ ] Update API documentation
- [ ] Create deployment guide
- [ ] Document search query syntax
- [ ] Add architecture diagrams

## Next Sprint Planning

### Week 1 (Immediate)
1. Complete MySQL migration
2. Fix failing tests
3. Deploy to dev environment

### Week 2-3
1. Implement OpenSearch consumer
2. Create basic search endpoints
3. Integration testing

### Week 4
1. Performance testing
2. Production readiness review
3. Documentation update

## Notes & Decisions

### Key Design Decisions
1. **MySQL over PostgreSQL**: Consistency with other services
2. **Event-callback pattern**: Ensures data freshness for indexing
3. **Caffeine cache for gRPC**: Prevents resource leaks
4. **Soft deletes**: Maintains audit trail and enables recovery
5. **Reactive programming**: Better resource utilization

### Risks & Mitigations
- **Risk**: Data inconsistency between DB and search
  - **Mitigation**: Event sourcing with replay capability
  
- **Risk**: Performance degradation with large datasets
  - **Mitigation**: Implement pagination and caching
  
- **Risk**: OpenSearch unavailability
  - **Mitigation**: Circuit breaker with database fallback

### Dependencies
- MySQL 8.0+
- OpenSearch 3.x
- Kafka for event streaming
- MinIO/S3 for file storage
- Consul for service discovery