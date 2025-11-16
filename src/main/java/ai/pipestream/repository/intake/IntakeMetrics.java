package ai.pipestream.repository.intake;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Metrics for Phase 1 intake operations.
 * Exposes counters, timers, and histograms via Micrometer.
 */
@ApplicationScoped
public class IntakeMetrics {

    @Inject
    MeterRegistry registry;

    private Counter uploadInitiatedTotal;
    private Counter uploadChunkReceivedTotal;
    private Counter uploadChunkIdempotentHitTotal;
    private Counter uploadStreamClientsTotal;

    private Timer rpcInitiateUploadLatency;
    private Timer rpcUploadChunkAckLatency;
    private Timer rpcGetStatusLatency;

    private DistributionSummary uploadChunkPayloadBytes;

    @PostConstruct
    void init() {
        // Counters
        uploadInitiatedTotal = Counter.builder("upload_initiated_total")
                .description("Total number of uploads initiated")
                .register(registry);

        uploadChunkReceivedTotal = Counter.builder("upload_chunk_received_total")
                .description("Total number of chunks received")
                .register(registry);

        uploadChunkIdempotentHitTotal = Counter.builder("upload_chunk_idempotent_hit_total")
                .description("Total number of duplicate chunks received without force flag")
                .register(registry);

        uploadStreamClientsTotal = Counter.builder("upload_stream_clients_total")
                .description("Total number of progress stream clients connected")
                .register(registry);

        // Timers
        rpcInitiateUploadLatency = Timer.builder("rpc_initiate_upload_latency_ms")
                .description("Latency of InitiateUpload RPC")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        rpcUploadChunkAckLatency = Timer.builder("rpc_upload_chunk_ack_latency_ms")
                .description("Latency of UploadChunk acknowledgment")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        rpcGetStatusLatency = Timer.builder("rpc_get_status_latency_ms")
                .description("Latency of GetUploadStatus RPC")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        // Distribution Summaries
        uploadChunkPayloadBytes = DistributionSummary.builder("upload_chunk_payload_bytes")
                .description("Distribution of chunk payload sizes in bytes")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public void recordUploadInitiated() {
        uploadInitiatedTotal.increment();
    }

    public void recordChunkReceived(long bytes) {
        uploadChunkReceivedTotal.increment();
        uploadChunkPayloadBytes.record(bytes);
    }

    public void recordIdempotentHit() {
        uploadChunkIdempotentHitTotal.increment();
    }

    public void recordStreamClient() {
        uploadStreamClientsTotal.increment();
    }

    public Timer.Sample startInitiateUploadTimer() {
        return Timer.start(registry);
    }

    public void stopInitiateUploadTimer(Timer.Sample sample) {
        sample.stop(rpcInitiateUploadLatency);
    }

    public Timer.Sample startUploadChunkTimer() {
        return Timer.start(registry);
    }

    public void stopUploadChunkTimer(Timer.Sample sample) {
        sample.stop(rpcUploadChunkAckLatency);
    }

    public Timer.Sample startGetStatusTimer() {
        return Timer.start(registry);
    }

    public void stopGetStatusTimer(Timer.Sample sample) {
        sample.stop(rpcGetStatusLatency);
    }
}
