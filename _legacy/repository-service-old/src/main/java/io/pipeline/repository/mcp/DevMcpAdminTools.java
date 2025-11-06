package io.pipeline.repository.mcp;

import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.PromptMessage;
import io.quarkiverse.mcp.server.Prompt;
import io.quarkiverse.mcp.server.PromptArg;

@IfBuildProfile("dev")
@ApplicationScoped
public class DevMcpAdminTools {

    private static final Set<String> SECRET_KEYS = Set.of("password", "secret", "token", "credential", "access-key", "accesskey");

    @Tool(description = "Set logger level for a category (TRACE, DEBUG, INFO, WARN, ERROR)")
    public String setLogLevel(String category, String level) {
        if (category == null || category.isBlank()) {
            return "Category must not be empty";
        }
        java.util.logging.Level julLevel = mapLevel(level);
        if (julLevel == null) {
            return "Unknown level: " + level;
        }
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(category);
        logger.setLevel(julLevel);
        return "Set log level for " + category + " to " + julLevel;
    }

    @Tool(description = "Get logger level for a category")
    public String getLogLevel(String category) {
        if (category == null || category.isBlank()) {
            return "Category must not be empty";
        }
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(category);
        java.util.logging.Level lvl = logger.getLevel();
        return (lvl == null ? "INHERITED" : lvl.getName());
    }

    @Tool(description = "Tail the dev log file; returns the last N lines")
    public String tailLogs(Integer lines) {
        int n = (lines == null || lines <= 0) ? 200 : Math.min(lines, 2000);
        Path logPath = getDevLogPath();
        if (logPath == null) {
            return "Dev log file is not configured or does not exist";
        }
        try {
            List<String> all = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            int from = Math.max(0, all.size() - n);
            return all.subList(from, all.size()).stream().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            return "Failed to read log file: " + e.getMessage();
        }
    }

    @Tool(description = "Dump effective config as key=value lines; optional prefix filter")
    public String configDump(String prefix) {
        Config cfg = ConfigProvider.getConfig();
        Map<String, String> entries = new TreeMap<>();
        for (String name : cfg.getPropertyNames()) {
            if (prefix != null && !prefix.isBlank() && !name.startsWith(prefix)) {
                continue;
            }
            String value = cfg.getOptionalValue(name, String.class).orElse("<unset>");
            if (isSecretKey(name)) {
                value = mask(value);
            }
            entries.put(name, value);
        }
        StringBuilder sb = new StringBuilder();
        entries.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        return sb.toString();
    }

    @Tool(description = "List known loggers and their levels; optional prefix filter")
    public String listLoggers(String prefix) {
        java.util.logging.LogManager lm = java.util.logging.LogManager.getLogManager();
        List<String> names = new ArrayList<>();
        lm.getLoggerNames().asIterator().forEachRemaining(names::add);
        names.sort(String::compareTo);
        StringBuilder sb = new StringBuilder();
        for (String n : names) {
            if (prefix != null && !prefix.isBlank() && !n.startsWith(prefix)) continue;
            java.util.logging.Level lvl = java.util.logging.Logger.getLogger(n).getLevel();
            sb.append(n).append('=').append(lvl == null ? "INHERITED" : lvl.getName()).append('\n');
        }
        return sb.toString();
    }

    @Tool(description = "Return recent lines containing ERROR/WARN from dev log file")
    public String recentErrors(Integer lines) {
        int n = (lines == null || lines <= 0) ? 200 : Math.min(lines, 2000);
        Path logPath = getDevLogPath();
        if (logPath == null) return "Dev log file is not configured or does not exist";
        try {
            List<String> all = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            int from = Math.max(0, all.size() - 5000); // scan last 5k lines for perf
            List<String> filtered = all.subList(from, all.size()).stream()
                .filter(l -> l.contains("ERROR") || l.contains("SEVERE") || l.contains("WARN"))
                .collect(Collectors.toList());
            int start = Math.max(0, filtered.size() - n);
            return String.join("\n", filtered.subList(start, filtered.size()));
        } catch (IOException e) {
            return "Failed to read log file: " + e.getMessage();
        }
    }

    @Tool(description = "GET /q/health from this service and return JSON")
    public String health() {
        String base = baseHttpUrl();
        return httpGet(base + "/q/health");
    }

    @Tool(description = "GET /q/metrics (if micrometer is enabled) and return text")
    public String metrics() {
        String base = baseHttpUrl();
        String r = httpGet(base + "/q/metrics");
        if (r != null && !r.startsWith("HTTP error")) return r;
        return "Metrics endpoint not available or disabled";
    }

    @Prompt(name = "service_state")
    public PromptMessage serviceState(@PromptArg(name = "verbose") String verbose) {
        boolean v = false;
        if (verbose != null) {
            String s = verbose.trim().toLowerCase(java.util.Locale.ROOT);
            v = ("1".equals(s) || "true".equals(s) || "yes".equals(s) || "y".equals(s));
        }
        Map<String, Object> state = collectState(v);
        String json = toJson(state);
        return PromptMessage.withUserRole(new TextContent(json));
    }

    private Map<String, Object> collectState(boolean verbose) {
        Config cfg = ConfigProvider.getConfig();
        Map<String, Object> m = new HashMap<>();
        m.put("application", cfg.getOptionalValue("quarkus.application.name", String.class).orElse("unknown"));
        m.put("profile", cfg.getOptionalValue("quarkus.profile", String.class).orElse("dev"));
        m.put("httpPort", cfg.getOptionalValue("quarkus.http.port", Integer.class).orElse(0));
        m.put("mcpRootPath", cfg.getOptionalValue("quarkus.mcp.server.sse.root-path", String.class).orElse("/mcp"));

        long now = System.currentTimeMillis();
        long start = ManagementFactory.getRuntimeMXBean().getStartTime();
        m.put("startTime", Instant.ofEpochMilli(start).toString());
        m.put("uptimeMillis", now - start);

        MemoryMXBean mx = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mx.getHeapMemoryUsage();
        Map<String, Object> mem = new HashMap<>();
        mem.put("init", heap.getInit());
        mem.put("used", heap.getUsed());
        mem.put("committed", heap.getCommitted());
        mem.put("max", heap.getMax());
        m.put("heap", mem);

        if (verbose) {
            m.put("javaVersion", System.getProperty("java.version"));
            m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
            m.put("user", System.getProperty("user.name"));
            m.put("threads", Thread.activeCount());
        }
        return m;
    }

    private static boolean isSecretKey(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return SECRET_KEYS.stream().anyMatch(lower::contains);
    }

    private static String mask(String value) {
        if (value == null || value.isBlank()) return value;
        int show = Math.min(4, value.length());
        return value.substring(0, show) + "***";
    }

    private static java.util.logging.Level mapLevel(String level) {
        if (level == null) return null;
        String s = level.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "TRACE" -> java.util.logging.Level.FINEST;
            case "DEBUG" -> java.util.logging.Level.FINE;
            case "INFO" -> java.util.logging.Level.INFO;
            case "WARN", "WARNING" -> java.util.logging.Level.WARNING;
            case "ERROR", "SEVERE" -> java.util.logging.Level.SEVERE;
            default -> {
                try { yield java.util.logging.Level.parse(s); } catch (Exception e) { yield null; }
            }
        };
    }

    private Path getDevLogPath() {
        Config cfg = ConfigProvider.getConfig();
        String path = cfg.getOptionalValue("quarkus.log.file.path", String.class).orElse("build/dev.log");
        Path p = Path.of(path);
        if (!p.isAbsolute()) {
            p = Path.of(".").resolve(p).normalize();
        }
        return Files.exists(p) ? p : null;
    }

    private String baseHttpUrl() {
        Config cfg = ConfigProvider.getConfig();
        String host = cfg.getOptionalValue("quarkus.http.host", String.class).orElse("127.0.0.1");
        int port = cfg.getOptionalValue("quarkus.http.port", Integer.class).orElse(8080);
        return "http://" + host + ":" + port;
    }

    private String httpGet(String url) {
        try {
            java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
            java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url)).GET().build();
            java.net.http.HttpResponse<String> resp = client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
            int code = resp.statusCode();
            if (code >= 200 && code < 300) return resp.body();
            return "HTTP error " + code + " from " + url;
        } catch (Exception e) {
            return "Request failed: " + e.getMessage();
        }
    }

    private static String toJson(Map<String, Object> map) {
        // Minimal JSON serializer to avoid extra deps in dev
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        List<Map.Entry<String, Object>> list = new ArrayList<>(map.entrySet());
        list.sort(Comparator.comparing(Map.Entry::getKey));
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<String, Object> e = list.get(i);
            sb.append('"').append(escape(e.getKey())).append('"').append(':');
            sb.append(valueToJson(e.getValue()));
            if (i < list.size() - 1) sb.append(',');
        }
        sb.append('}');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static String valueToJson(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return String.valueOf(v);
        if (v instanceof Map<?, ?> m) {
            Map<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                sorted.put(String.valueOf(e.getKey()), e.getValue());
            }
            return toJson(sorted);
        }
        return '"' + escape(String.valueOf(v)) + '"';
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
