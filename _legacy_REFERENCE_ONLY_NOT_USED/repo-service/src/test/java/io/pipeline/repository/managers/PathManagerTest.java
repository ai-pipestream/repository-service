package io.pipeline.repository.managers;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class PathManagerTest {

    @Inject
    PathManager pathManager;

    @Test
    void testBuildS3KeyBasic() {
        String result = pathManager.buildS3Key("test-connector", null, "abc-123");
        assertEquals("connectors/test-connector/abc-123.pb", result);
    }

    @Test
    void testBuildS3KeyWithPath() {
        String result = pathManager.buildS3Key("test-connector", "docs/2024", "abc-123");
        assertEquals("connectors/test-connector/docs/2024/abc-123.pb", result);
    }

    @Test
    void testBuildS3KeyWithSlashes() {
        String result = pathManager.buildS3Key("test-connector", "/docs/2024/", "abc-123");
        assertEquals("connectors/test-connector/docs/2024/abc-123.pb", result);
    }

    @Test
    void testBuildS3KeyNullConnector() {
        String result = pathManager.buildS3Key(null, "path", "abc-123");
        assertEquals("connectors/default/path/abc-123.pb", result);
    }

    @Test
    void testParseS3Key() {
        PathManager.S3KeyComponents result = pathManager.parseS3Key("connectors/test-connector/docs/2024/abc-123.pb");
        assertEquals("test-connector", result.connectorId);
        assertEquals("docs/2024", result.path);
        assertEquals("abc-123", result.nodeId);
    }

    @Test
    void testParseS3KeyNoPath() {
        PathManager.S3KeyComponents result = pathManager.parseS3Key("connectors/test-connector/abc-123.pb");
        assertEquals("test-connector", result.connectorId);
        assertEquals("", result.path);
        assertEquals("abc-123", result.nodeId);
    }

    @Test
    void testValidConnectorId() {
        assertTrue(pathManager.isValidConnectorId("test-connector"));
        assertTrue(pathManager.isValidConnectorId("test_connector_123"));
        assertFalse(pathManager.isValidConnectorId("test connector"));
        assertFalse(pathManager.isValidConnectorId(""));
        assertFalse(pathManager.isValidConnectorId(null));
    }

    @Test
    void testValidPath() {
        assertTrue(pathManager.isValidPath("docs/2024"));
        assertTrue(pathManager.isValidPath("simple"));
        assertTrue(pathManager.isValidPath(null));
        assertTrue(pathManager.isValidPath(""));
        assertFalse(pathManager.isValidPath("docs with spaces"));
    }
}