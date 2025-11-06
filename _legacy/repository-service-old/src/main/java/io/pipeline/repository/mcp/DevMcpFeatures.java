package io.pipeline.repository.mcp;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkiverse.mcp.server.Prompt;
import io.quarkiverse.mcp.server.PromptArg;
import io.quarkiverse.mcp.server.PromptMessage;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.Tool;

/**
 * Minimal MCP features available only in dev to support live-coding with chat tools.
 */
@IfBuildProfile("dev")
@ApplicationScoped
public class DevMcpFeatures {

    @Tool(description = "Converts the string value to lower case")
    public String toLowerCase(String value) {
        return value == null ? null : value.toLowerCase();
    }

    @Prompt(name = "hello")
    public PromptMessage hello(@PromptArg(name = "name") String name) {
        String msg = "Hello, " + (name == null || name.isBlank() ? "developer" : name) + "!";
        return PromptMessage.withUserRole(new TextContent(msg));
    }
}

