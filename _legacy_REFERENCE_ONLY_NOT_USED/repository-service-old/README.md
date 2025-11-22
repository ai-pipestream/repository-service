# PipeDoc Repository Service

A Quarkus-based gRPC service providing CRUD operations f or PipeDoc and ModuleProcessRequest storage using MinIO, PostgreSQL, and OpenSearch..

## Features

- **gRPC Service**: Full implementation of the PipeDocRepository service defined in:
  - Services
    - [filesystem_service.proto](../../grpc-stubs/src/main/proto/filesystem_service.proto)
    - [module_repository_service.proto](../../grpc-stubs/src/main/proto/module_repository_service.proto)
    - [graph_repository_service.proto](../../grpc-stubs/src/main/proto/graph_repository_service.proto)
    - [pipedoc_repository_service.proto](../../grpc-stubs/src/main/proto/pipedoc_repository_service.proto)
    - [process_request_repository_service.proto](../../grpc-stubs/src/main/proto/process_request_repository_service.proto)
  - Protobufs used: 
    - [repository_service_data.proto](../../grpc-stubs/src/main/proto/repository_service_data.proto)
    - [pipeline_core_types.proto](../../grpc-stubs/src/main/proto/pipeline_core_types.proto)
    - TODO: add more
- **Dev Services**: Automatic Redis container provisioning in development mode
- **Health Checks**: Built-in health endpoints for monitoring
- **Quinoa Integration**: Ready for Node.js frontend integration (and we will attempt a backend)

## Running in Development

```bash
# From the project root directory
./gradlew :applications:pipedoc-repository-service:quarkusDev
```

The service will start with:
- HTTP endpoint: http://localhost:38002
- gRPC endpoint: localhost:38003

### MCP Server (dev)

The Repository Service now includes the Quarkus MCP Server (SSE transport) for live-coding with chat tools:

- SSE endpoint: `http://localhost:38102/mcp/sse`
- Root path: `http://localhost:38102/mcp`
- Dev traffic logging: enabled to help debug JSON traffic

To connect from an MCP-capable client (e.g., LangChain4j MCP client or an IDE Chat plugin), point it to the SSE endpoint above while this service runs in dev mode. For configuration options, see:

- `reference-code/quarkus-mcp-server/docs/modules/ROOT/pages/index.adoc`
- `reference-code/quarkus-mcp-server/docs/modules/ROOT/pages/cli-adapter.adoc`
- `reference-code/quarkus-mcp-server/docs/modules/ROOT/nav.adoc`

#### Using Codex (stdio-only clients)

Codex currently launches MCP servers over stdio. Use the provided stdio→SSE proxy:

1) Start this service in dev: `./gradlew :applications:pipedoc-repository-service:quarkusDev`
2) In a separate terminal, run: `./scripts/mcp-proxy.sh` (or pass a custom URL, e.g. `./scripts/mcp-proxy.sh http://localhost:38102/mcp`)
3) In `~/.codex/config.toml`, add:

```
[mcp_servers.pipedoc-repo-dev]
command = "jbang"
args = ["io.quarkiverse.mcp:quarkus-mcp-stdio-sse-proxy:RELEASE", "http://localhost:38102/mcp/sse"]
```

If you don’t have jbang, the script will fall back to a locally built proxy JAR. Build it with:

```
pushd reference-code/quarkus-mcp-server/devtools/stdio-sse-proxy
  ./mvnw -q -DskipTests package
popd
```

## Service Endpoints

### gRPC Operations

TODO: this needs to be updated 

### Health Check

- Health endpoint: http://localhost:38002/q/health
- Liveness probe: http://localhost:38002/q/health/live
- Readiness probe: http://localhost:38002/q/health/ready

## Components
* S3 for binary protobuf storage
* PostgreSQL for metadta lookups and ACID compliance
* Kafka for event update streaming to index data
* OpenSearch for fast search capabilties of metadata / assist in calls that are too slow for a database or an S3 query.

## Implementation guidelines
Given that data is going to be stored in S3, PostgreSQL, and OpenSearch - the functions in the API are going to utilize this to create it's API calls.

The postgres database will be used for metadata lookups and ACID compliance.  The S3 bucket will be used for binary protobuf storage.  The OpenSearch database will be used for fast search capabilties of metadata / assist in calls that are too slow for a database or an S3 query.

The development of this service should choose the best of breed solution for each of the API calls to be implemented.

## Service calls

* The filesystem service, `io.pipeline.repository.filesystem.FilesystemService` is used to store the binary protobufs.  
* The module repository service,  is used to store the metadata.  The graph repository service is used to store the graph data.  
* The pipedoc repository service is used to store the PipeDoc data.  
* The process request repository service is used to store the ModuleProcessRequest data.


## Configuration

Key configuration properties in `application.properties`:

```properties
## TODO: update configuration , used shared port.

```

## Building

```bash
# Build from project root
./gradlew :applications:pipedoc-repository-service:build

# Build native image (requires GraalVM)
./gradlew :applications:pipedoc-repository-service:build -Dquarkus.native.enabled=true
```

## Integration with Dev Tools

This service is designed to work with the Pipeline Developer Tools, sharing the same MongoDB instance when both are running locally.

### Architecture

- **Desktop Dev Tools (Node.js)**: Connects via gRPC only (port 38003)
- **Quinoa Frontend**: Uses REST endpoints (port 38002) for future web UI
- **Docker Compose Services**: 
### Running with Dev Tools

1. Start this repository service: `./gradlew :applications:pipedoc-repository-service:quarkusDev`
   - Quarkus Dev Services will automatically start a MinIO, kafka, apicurio, and postgreSQL containers labeled (TODO: look this up and figure it out)
2. Quarkus quinoa starts the frontend and backend services appropriately because of the quinoa setup (NOTE: may need to research how to get the backend going too?  It would be useful if it can do both) 
