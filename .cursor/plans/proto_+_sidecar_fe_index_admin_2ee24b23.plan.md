---
name: Proto + sidecar FE index admin
overview: ListIndices enriched with VectorFieldSummary (1b), module-testing-sidecar calls opensearch-manager via dynamic-gRPC (same REST paths for Quinoa), FE shows vector-set bindings and updated error hints.
todos:
  - id: proto-sync
    content: pipestream-protos committed; git-proto-workspace consumers run clean fetchProtos/generateProtos
    status: completed
  - id: proto-index-metadata
    content: VectorFieldSummary + vector_fields on OpenSearchIndexInfo; ListIndices + VectorSetIndexBinding join
    status: completed
  - id: sidecar-gradle-protos
    content: module-testing-sidecar pipestreamProtos local + schemamanager + opensearch + quarkus grpc codegen off
    status: completed
  - id: sidecar-dynamic-grpc
    content: application.properties opensearch-manager-service + dynamic-grpc dev address; removed HTTP URL
    status: completed
  - id: sidecar-rest-grpc
    content: IndexAdminResource (gRPC) replaces IndexAdminProxyResource + delete safety parity
    status: completed
  - id: fe-app-vue
    content: App.vue vector column + dynamic-grpc error hints
    status: completed
  - id: e2e-verify
    content: Manual check index admin against running opensearch-manager + Consul
    status: completed
isProject: false
---

# Index admin: proto, manager, sidecar REST, FE

## Implemented baseline

- **[pipestream-protos](file:///work/core-services/pipestream-protos/opensearch/proto/ai/pipestream/opensearch/v1/opensearch_manager.proto):** `VectorFieldSummary`; `OpenSearchIndexInfo.vector_fields` (field 5); plus existing `GetIndexMapping`, `CreateIndexRequest.vector_field_name`, etc.
- **[opensearch-manager](file:///work/core-services/opensearch-manager):** `OpenSearchIndexingService.listIndices` merges async `_cat/indices` with `VectorSetIndexBindingEntity.findAllByIndexNames`; REST `IndexAdminResource` exposes `vectorFields` in JSON when present. `getIndexMapping` completionStage uses CompletableFuture directly (no erroneous `.toCompletionStage()`); IOException wrapped.
- **[module-testing-sidecar](file:///work/modules/module-testing-sidecar):** **[IndexAdminResource](file:///work/modules/module-testing-sidecar/src/main/java/ai/pipestream/module/pipelineprobe/IndexAdminResource.java)** uses `DynamicGrpcClientFactory` + `MutinyOpenSearchManagerServiceGrpc`; [build.gradle](file:///work/modules/module-testing-sidecar/build.gradle) registers `opensearch` + `schemamanager`, local proto path fallback `../../core-services/pipestream-protos`.

## Architecture

```mermaid
flowchart TD
  subgraph browser [Browser]
    Quinoa[Quinoa App.vue]
  end
  subgraph sidecar [module-testing-sidecar]
    REST["REST /test-sidecar/v1/indices"]
    DG[DynamicGrpcClientFactory]
  end
  subgraph manager [opensearch-manager]
    OSM[OpenSearchManagerService gRPC]
    OS[OpenSearch cluster]
  end
  Quinoa --> REST
  REST --> DG
  DG -->|"opensearch-manager"| OSM
  OSM --> OS
```

## 1b — VectorFieldSummary (in scope)

- Proto: `VectorFieldSummary` + `repeated vector_fields` on `OpenSearchIndexInfo`.
- Manager: DB join via `VectorSetIndexBindingEntity.findAllByIndexNames` after cat response; summaries from `VectorSetEntity` (id, name, field_name, result_set_name, dimensions).

## Remaining

- **E2E:** Run pipeline + sidecar Index Admin; confirm `vectorFields` appears for indices with bindings.
