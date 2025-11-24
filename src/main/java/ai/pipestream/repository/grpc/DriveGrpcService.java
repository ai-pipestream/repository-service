package ai.pipestream.repository.grpc;

import ai.pipestream.repository.entity.Drive;
import ai.pipestream.repository.filesystem.*;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@GrpcService
public class DriveGrpcService implements FilesystemService {

    private static final Logger LOG = Logger.getLogger(DriveGrpcService.class);

    @Override
    @Transactional
    public Uni<ai.pipestream.repository.filesystem.Drive> createDrive(CreateDriveRequest request) {
        return Uni.createFrom().item(() -> {
            if (Drive.findByName(request.getName()) != null) {
                throw new io.grpc.StatusRuntimeException(io.grpc.Status.ALREADY_EXISTS
                        .withDescription("Drive with name " + request.getName() + " already exists"));
            }

            Drive drive = new Drive();
            drive.name = request.getName();
            drive.bucketName = request.getBucketName();
            drive.accountId = request.getAccountId();
            drive.region = request.getRegion();
            drive.credentialsRef = request.getCredentialsRef();
            drive.statusId = request.getStatusId();
            drive.description = request.getDescription();
            drive.metadata = request.getMetadata();
            drive.createdAt = Instant.now();

            drive.persist();
            LOG.infof("Created drive: %s (Bucket: %s)", drive.name, drive.bucketName);
            return mapToProto(drive);
        });
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Drive> getDrive(GetDriveRequest request) {
        return Uni.createFrom().item(() -> {
            Drive drive = Drive.findByName(request.getName());
            if (drive == null) {
                throw new io.grpc.StatusRuntimeException(io.grpc.Status.NOT_FOUND
                        .withDescription("Drive not found: " + request.getName()));
            }
            return mapToProto(drive);
        });
    }

    @Override
    public Uni<ListDrivesResponse> listDrives(ListDrivesRequest request) {
        return Uni.createFrom().item(() -> {
            // Basic listing for now, ignoring pagination/filtering
            List<Drive> drives = Drive.listAll();
            List<ai.pipestream.repository.filesystem.Drive> protoDrives = drives.stream()
                    .map(this::mapToProto)
                    .collect(Collectors.toList());

            return ListDrivesResponse.newBuilder()
                    .addAllDrives(protoDrives)
                    .setTotalCount(drives.size())
                    .build();
        });
    }

    @Override
    @Transactional
    public Uni<DeleteDriveResponse> deleteDrive(DeleteDriveRequest request) {
        return Uni.createFrom().item(() -> {
            if (!"DELETE_DRIVE_DATA".equals(request.getConfirmation())) {
                throw new io.grpc.StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Confirmation string must be DELETE_DRIVE_DATA"));
            }

            Drive drive = Drive.findByName(request.getName());
            if (drive == null) {
                throw new io.grpc.StatusRuntimeException(io.grpc.Status.NOT_FOUND
                        .withDescription("Drive not found: " + request.getName()));
            }

            drive.delete();
            LOG.infof("Deleted drive: %s", request.getName());

            return DeleteDriveResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Drive deleted successfully")
                    .build();
        });
    }

    // Implement other methods as NOT_IMPLEMENTED for now
    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> createNode(CreateNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> getNode(GetNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> getNodeByPath(GetNodeByPathRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> updateNode(UpdateNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<DeleteNodeResponse> deleteNode(DeleteNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<GetChildrenResponse> getChildren(GetChildrenRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<GetPathResponse> getPath(GetPathRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<GetTreeResponse> getTree(GetTreeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> moveNode(MoveNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ai.pipestream.repository.filesystem.Node> copyNode(CopyNodeRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<SearchNodesResponse> searchNodes(SearchNodesRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<SearchDrivesResponse> searchDrives(SearchDrivesRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public io.smallrye.mutiny.Multi<MetadataExport> streamAllMetadata(StreamMetadataRequest request) {
        return io.smallrye.mutiny.Multi.createFrom()
                .failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<FormatFilesystemResponse> formatFilesystem(FormatFilesystemRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ListDriveBucketStatusResponse> listDriveBucketStatus(ListDriveBucketStatusRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    @Override
    public Uni<ReindexPipeDocsResponse> reindexPipeDocs(ReindexPipeDocsRequest request) {
        return Uni.createFrom().failure(new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED));
    }

    private ai.pipestream.repository.filesystem.Drive mapToProto(Drive entity) {
        ai.pipestream.repository.filesystem.Drive.Builder builder = ai.pipestream.repository.filesystem.Drive
                .newBuilder()
                .setId(entity.id)
                .setName(entity.name)
                .setBucketName(entity.bucketName)
                .setAccountId(entity.accountId);

        if (entity.region != null)
            builder.setRegion(entity.region);
        if (entity.credentialsRef != null)
            builder.setCredentialsRef(entity.credentialsRef);
        if (entity.statusId != null)
            builder.setStatusId(entity.statusId);
        if (entity.description != null)
            builder.setDescription(entity.description);
        if (entity.metadata != null)
            builder.setMetadata(entity.metadata);

        if (entity.createdAt != null) {
            builder.setCreatedAt(Timestamp.newBuilder()
                    .setSeconds(entity.createdAt.getEpochSecond())
                    .setNanos(entity.createdAt.getNano())
                    .build());
        }

        return builder.build();
    }
}
