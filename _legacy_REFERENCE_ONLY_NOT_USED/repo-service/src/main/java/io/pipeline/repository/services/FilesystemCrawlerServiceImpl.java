package io.pipeline.repository.services;

import io.pipeline.data.v1.Blob;
import io.pipeline.data.v1.BlobBag;
import io.pipeline.data.v1.PipeDoc;
import io.pipeline.data.v1.SearchMetadata;
import io.pipeline.repository.crawler.*;
import io.pipeline.repository.filesystem.CreateNodeRequest;
import io.pipeline.repository.services.FilesystemServiceImpl;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.inject.Default;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * @deprecated This service is for testing only. Production crawlers should be separate services.
 * TODO: Move to dedicated crawler-service application
 */
@Deprecated
@GrpcService
@Default
public class FilesystemCrawlerServiceImpl extends MutinyFilesystemCrawlerServiceGrpc.FilesystemCrawlerServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FilesystemCrawlerServiceImpl.class);

    @Inject
    FilesystemServiceImpl filesystemService;

    @Override
    @ActivateRequestContext // Fix for Quarkus bug #8868: ConcurrentModificationException with async database operations
    public Uni<CrawlDirectoryResponse> crawlDirectory(CrawlDirectoryRequest request) {
        log.info("Crawling directory: {} (recursive: {})", request.getPath(), request.getRecursive());

        if (request.getPath() == null || request.getPath().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Path is required"));
        }
        if (request.getDrive() == null || request.getDrive().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Drive is required"));
        }
        if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Connector ID is required"));
        }

        Path startPath = Paths.get(request.getPath());
        if (!Files.exists(startPath)) {
            return Uni.createFrom().failure(new IllegalArgumentException("Path does not exist: " + request.getPath()));
        }
        if (!Files.isDirectory(startPath)) {
            return Uni.createFrom().failure(new IllegalArgumentException("Path is not a directory: " + request.getPath()));
        }

        try (Stream<Path> paths = request.getRecursive() ?
             Files.walk(startPath) :
             Files.list(startPath)) {

            List<Path> filePaths = paths
                .filter(Files::isRegularFile)
                .toList();

            int filesFound = filePaths.size();
            log.info("Found {} files to process", filesFound);

            // Process files reactively using Multi (now thread-safe with standard Hibernate)
            return Multi.createFrom().iterable(filePaths)
                .onItem().transformToUni(filePath ->
                    processFileReactively(filePath, startPath, request.getDrive(), request.getConnectorId())
                )
                .merge()
                .collect().asList()
                .map(results -> {
                    List<String> processedFiles = new ArrayList<>();
                    List<String> failedFiles = new ArrayList<>();

                    for (ProcessFileResult result : results) {
                        if (result.success) {
                            processedFiles.add(result.filePath);
                            log.debug("Processed file: {}", result.filePath);
                        } else {
                            failedFiles.add(result.filePath);
                            log.error("Failed to process file: {}: {}", result.filePath, result.error);
                        }
                    }

                    return CrawlDirectoryResponse.newBuilder()
                        .setFilesFound(filesFound)
                        .setFilesProcessed(processedFiles.size())
                        .setFilesFailed(failedFiles.size())
                        .addAllProcessedFiles(processedFiles)
                        .addAllFailedFiles(failedFiles)
                        .build();
                });
        } catch (IOException e) {
            return Uni.createFrom().failure(new RuntimeException("Failed to walk directory: " + e.getMessage(), e));
        }
    }

    private PipeDoc createPipeDocFromFile(Path filePath) throws IOException {
        // Read file content
        byte[] fileContent = Files.readAllBytes(filePath);
        String fileName = filePath.getFileName().toString();
        String mimeType = Files.probeContentType(filePath);
        if (mimeType == null) {
            mimeType = "application/octet-stream";
        }

        // Get file metadata
        File file = filePath.toFile();
        long lastModified = file.lastModified();

        // Create a unique doc ID
        String docId = UUID.randomUUID().toString();

        // Build SearchMetadata
        SearchMetadata.Builder metadataBuilder = SearchMetadata.newBuilder()
            .setTitle(fileName)
            .setSourceUri("file://" + filePath.toAbsolutePath())
            .setSourceMimeType(mimeType)
            .setContentLength((int) fileContent.length)
            .setProcessedDate(Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .build());

        if (lastModified > 0) {
            metadataBuilder.setLastModifiedDate(Timestamp.newBuilder()
                .setSeconds(lastModified / 1000)
                .build());
        }

        // Determine document type from extension
        String docType = getDocumentType(fileName);
        if (docType != null) {
            metadataBuilder.setDocumentType(docType);
        }

        // If it's a text file, we can add the content as body
        if (mimeType != null && mimeType.startsWith("text/")) {
            String textContent = new String(fileContent);
            if (textContent.length() > 10000) {
                // Truncate for metadata, full content is in blob
                metadataBuilder.setBody(textContent.substring(0, 10000) + "...");
            } else {
                metadataBuilder.setBody(textContent);
            }
        }

        // Create Blob with file content
        Blob blob = Blob.newBuilder()
            .setBlobId(UUID.randomUUID().toString())
            .setData(ByteString.copyFrom(fileContent))
            .setMimeType(mimeType)
            .setFilename(fileName)
            .setSizeBytes(fileContent.length)
            .build();

        // Create BlobBag
        BlobBag blobBag = BlobBag.newBuilder()
            .setBlob(blob)
            .build();

        // Build PipeDoc
        return PipeDoc.newBuilder()
            .setDocId(docId)
            .setSearchMetadata(metadataBuilder.build())
            .setBlobBag(blobBag)
            .build();
    }

    private String getDocumentType(String fileName) {
        String lowerName = fileName.toLowerCase();
        if (lowerName.endsWith(".pdf")) return "pdf";
        if (lowerName.endsWith(".doc") || lowerName.endsWith(".docx")) return "word";
        if (lowerName.endsWith(".xls") || lowerName.endsWith(".xlsx")) return "excel";
        if (lowerName.endsWith(".ppt") || lowerName.endsWith(".pptx")) return "powerpoint";
        if (lowerName.endsWith(".txt")) return "text";
        if (lowerName.endsWith(".md")) return "markdown";
        if (lowerName.endsWith(".html") || lowerName.endsWith(".htm")) return "html";
        if (lowerName.endsWith(".xml")) return "xml";
        if (lowerName.endsWith(".json")) return "json";
        if (lowerName.endsWith(".csv")) return "csv";
        if (lowerName.endsWith(".java")) return "java";
        if (lowerName.endsWith(".py")) return "python";
        if (lowerName.endsWith(".js")) return "javascript";
        if (lowerName.endsWith(".ts")) return "typescript";
        if (lowerName.endsWith(".cpp") || lowerName.endsWith(".cc")) return "cpp";
        if (lowerName.endsWith(".c")) return "c";
        if (lowerName.endsWith(".h") || lowerName.endsWith(".hpp")) return "header";
        if (lowerName.endsWith(".go")) return "go";
        if (lowerName.endsWith(".rs")) return "rust";
        if (lowerName.endsWith(".sql")) return "sql";
        if (lowerName.endsWith(".sh")) return "shell";
        if (lowerName.endsWith(".yaml") || lowerName.endsWith(".yml")) return "yaml";
        if (lowerName.endsWith(".properties")) return "properties";
        if (lowerName.endsWith(".proto")) return "protobuf";
        return null;
    }

    private Uni<ProcessFileResult> processFileReactively(Path filePath, Path basePath, String drive, String connectorId) {
        return Uni.createFrom().item(() -> {
            try {
                // Create PipeDoc from file
                return createPipeDocFromFile(filePath);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create PipeDoc for " + filePath, e);
            }
        })
        .flatMap(pipeDoc -> {
            // Extract actual filename and relative path
            String fileName = filePath.getFileName().toString();
            String relativePath = basePath.relativize(filePath.getParent()).toString();
            if (relativePath.equals(".")) {
                relativePath = ""; // Root directory
            }

            // Create FilesystemService request with proper path
            CreateNodeRequest createRequest = CreateNodeRequest.newBuilder()
                .setDrive(drive)
                .setDocumentId(pipeDoc.getDocId())
                .setConnectorId(connectorId)
                .setName(fileName)
                .setPath(relativePath)
                .setContentType("application/x-protobuf")
                .setPayload(com.google.protobuf.Any.pack(pipeDoc))
                .setType(io.pipeline.repository.filesystem.Node.NodeType.FILE)
                .setNodeTypeId(io.pipeline.repository.filesystem.Node.NodeType.FILE.getNumber())
                .build();

            return filesystemService.createNode(createRequest)
                .map(response -> new ProcessFileResult(filePath.toString(), true, null))
                .onFailure().recoverWithItem(throwable ->
                    new ProcessFileResult(filePath.toString(), false, throwable.getMessage())
                );
        });
    }

    private static class ProcessFileResult {
        final String filePath;
        final boolean success;
        final String error;

        ProcessFileResult(String filePath, boolean success, String error) {
            this.filePath = filePath;
            this.success = success;
            this.error = error;
        }
    }
}