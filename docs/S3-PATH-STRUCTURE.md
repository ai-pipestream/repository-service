# S3 Path Structure and Document Lifecycle

This document describes the S3 object key structure used by the repository service and how it reflects the document lifecycle from intake through cluster processing.

## Path Structure Overview

The S3 path structure is organized hierarchically to group all states of a document together while maintaining clear separation between intake and cluster processing:

```
{prefix}/{account}/{connector}/{datasource}/{docId}/
  ├── intake/
  │   └── {uuid}.pb          # Initial intake document (graph_address_id = datasource_id)
  └── {clusterId}/
      ├── {uuid-1}.pb        # Document state at node-1 (graph_address_id = node-id)
      ├── {uuid-2}.pb        # Document state at node-2 (graph_address_id = node-id)
      └── {uuid-N}.pb        # Document state at node-N (graph_address_id = node-id)
```

## Path Components

### Base Path
- `{prefix}`: Configurable key prefix (default: `uploads`)
- `{account}`: Account identifier (account owner of the data)
- `{connector}`: Connector type that ingested the document (e.g., "s3", "file-crawler")
- `{datasource}`: Datasource ID (deterministic hash of account_id + connector_id)
- `{docId}`: Document identifier (logical document ID across all pipeline states)

### Intake Path
- `intake/`: Subdirectory for initial intake documents
- `{uuid}.pb`: UUID-based filename for the intake PipeDoc protobuf
  - UUID is deterministic: `UUID(doc_id, datasource_id, account_id)`
  - `graph_address_id` in DB record = `datasource_id`
  - `cluster_id` in DB record = `NULL`

**Example:**
```
uploads/account123/connector456/datasource789/doc-abc/intake/a1b2c3d4-e5f6-7890-abcd-ef1234567890.pb
```

### Cluster Processing Path
- `{clusterId}/`: Subdirectory for documents processed by a specific cluster
- `{uuid}.pb`: UUID-based filename for each node state
  - UUID is deterministic: `UUID(doc_id, node_id, account_id)`
  - `graph_address_id` in DB record = `node_id` (graph node that processed this state)
  - `cluster_id` in DB record = `{clusterId}`

**Example:**
```
uploads/account123/connector456/datasource789/doc-abc/cluster-prod/b2c3d4e5-f6a7-8901-bcde-f12345678901.pb
uploads/account123/connector456/datasource789/doc-abc/cluster-prod/c3d4e5f6-a7b8-9012-cdef-123456789012.pb
uploads/account123/connector456/datasource789/doc-abc/cluster-staging/d4e5f6a7-b8c9-0123-def0-234567890123.pb
```

## Document Lifecycle

### 1. Intake (Once per Document)

When a document first enters the system via a datasource:

1. Document is ingested (HTTP upload, connector intake, etc.)
2. PipeDoc is created with `graph_address_id = datasource_id`
3. Stored at: `{prefix}/{account}/{connector}/{datasource}/{docId}/intake/{uuid}.pb`
4. Database record created:
   - `graph_address_id = datasource_id`
   - `cluster_id = NULL`

**Key Point:** There is exactly **one intake record per `doc_id`** per datasource. The UUID ensures uniqueness even if the same logical document is ingested multiple times (idempotency is handled via checksum).

### 2. Cluster Processing (Multiple Clusters Possible)

After intake, documents can be routed to one or more clusters for processing:

1. Engine processes document through pipeline nodes
2. Each node that requires persistence (Kafka edges) saves PipeDoc
3. Stored at: `{prefix}/{account}/{connector}/{datasource}/{docId}/{clusterId}/{uuid}.pb`
4. Database record created/updated:
   - `graph_address_id = node_id` (the graph node that processed this state)
   - `cluster_id = {clusterId}`

**Key Points:**
- Same document can be processed by **multiple clusters** (each gets its own subdirectory)
- Each **node state** within a cluster gets its own UUID.pb file
- Documents processed via **pure gRPC edges** may never hit S3 (no persistence required)

## Multi-Cluster Processing

The path structure supports the use case where a datasource feeds into multiple clusters:

```
datasource-789 → cluster-prod    (production processing)
              → cluster-staging  (staging/testing)
              → cluster-dev      (development)
```

All cluster-processed states for the same document are grouped under their respective cluster subdirectories, making it easy to:
- Compare processing results across clusters
- Debug cluster-specific issues
- Audit document flow through different environments

## Recrawling and Cluster Seeding

The path structure enables powerful recrawling and cluster seeding scenarios:

### Seeding a New Cluster

When creating a new cluster, you can instantly seed it with existing data:

1. **Subscribe to intake data**: New cluster can subscribe to all intake topics for a datasource
2. **Replay from intake**: All `intake/{uuid}.pb` files can be rehydrated and reprocessed
3. **Cluster-specific processing**: New cluster processes documents and creates its own `{clusterId}/{uuid}.pb` files

### Recrawling

Recrawling is straightforward because:
- Original intake documents are preserved in `intake/` subdirectory
- Can replay from any point in the pipeline
- Cluster-specific processing doesn't affect intake records

## Pure gRPC Processing (No S3)

**Important:** Documents can flow through the entire pipeline **without ever hitting S3** when:

- All edges use **gRPC transport** (not Kafka)
- Documents are **small enough** for inline gRPC
- **No persistence is required** (no Kafka hops)

In this case:
- Document flows: `Intake → Engine (gRPC) → Node1 (gRPC) → Node2 (gRPC) → ...`
- No `SavePipeDoc` calls are made
- No S3 objects are created
- Processing is ephemeral and fast

Only when a **Kafka edge** is encountered does the Engine call `SavePipeDoc`, which:
1. Persists the PipeDoc to S3 using the path structure above
2. Creates/updates the database record
3. Publishes a DocumentReference to Kafka

## UUID Generation

The UUID serves as both the S3 filename and the database primary key (`node_id`). It is **deterministic** based on:

- **Intake**: `UUID(doc_id, datasource_id, account_id)`
- **Cluster**: `UUID(doc_id, node_id, account_id)`

This ensures:
- **Idempotency**: Same inputs always produce the same UUID
- **Uniqueness**: Different graph locations produce different UUIDs
- **Kafka partitioning**: UUID can be used as Kafka key for consistent partitioning

## Benefits of This Structure

1. **Organized by Document**: All states of a document are in one directory
2. **Clear Separation**: Intake vs. cluster processing is explicit
3. **Multi-Cluster Support**: Multiple clusters can process the same document
4. **Easy Seeding**: New clusters can replay from intake data
5. **Audit Trail**: Complete history of document processing is visible
6. **No Escaping Issues**: UUIDs are URL-safe (no special characters)
7. **No Collisions**: UUID ensures uniqueness even if doc_ids collide

## Database Schema Implications

The `pipedocs` table tracks metadata for each S3 object:

```sql
CREATE TABLE pipedocs (
  node_id UUID PRIMARY KEY,              -- Deterministic UUID (matches S3 filename)
  doc_id VARCHAR NOT NULL,                -- Logical document ID
  graph_address_id VARCHAR NOT NULL,     -- datasource_id (intake) or node_id (cluster)
  cluster_id VARCHAR,                     -- NULL for intake, cluster name for processing
  account_id VARCHAR NOT NULL,
  datasource_id VARCHAR NOT NULL,
  connector_id VARCHAR,
  object_key VARCHAR NOT NULL,            -- Full S3 path
  pipedoc_object_key VARCHAR NOT NULL,    -- Same as object_key for PipeDoc storage
  -- ... other metadata fields
);
```

**Query Patterns:**
- Find all states of a document: `WHERE doc_id = ?`
- Find intake document: `WHERE doc_id = ? AND cluster_id IS NULL`
- Find all cluster-processed states: `WHERE doc_id = ? AND cluster_id IS NOT NULL`
- Find states for a specific cluster: `WHERE doc_id = ? AND cluster_id = ?`

## Implementation Notes

- **Repository Service** owns all path building logic
- **Engine** never sees S3 paths (only DocumentReference)
- **Path sanitization** handles special characters in account/connector/datasource/doc_id
- **Cluster ID** comes from Engine configuration (`pipestream.cluster.id`)
- **Intake path** uses literal `"intake"` subdirectory name
- **Cluster path** uses the actual cluster ID as subdirectory name

