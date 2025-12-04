package com.databricks.jdbc.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/** Immutable report containing all collected metrics with statistical analysis. */
public class MetricsReport {
  private final long featureFlagsCallCount;
  private final LatencyStats featureFlagsStats;

  private final long connectionCreationCount;
  private final LatencyStats connectionCreationStats;

  private final long statementExecutionCount;
  private final LatencyStats statementExecutionStats;

  public MetricsReport(
      long featureFlagsCallCount,
      Queue<Long> featureFlagsLatencies,
      long connectionCreationCount,
      Queue<Long> connectionCreationLatencies,
      long statementExecutionCount,
      Queue<Long> statementExecutionLatencies) {
    this.featureFlagsCallCount = featureFlagsCallCount;
    this.featureFlagsStats = new LatencyStats(featureFlagsLatencies);

    this.connectionCreationCount = connectionCreationCount;
    this.connectionCreationStats = new LatencyStats(connectionCreationLatencies);

    this.statementExecutionCount = statementExecutionCount;
    this.statementExecutionStats = new LatencyStats(statementExecutionLatencies);
  }

  public long getFeatureFlagsCallCount() {
    return featureFlagsCallCount;
  }

  public LatencyStats getFeatureFlagsStats() {
    return featureFlagsStats;
  }

  public long getConnectionCreationCount() {
    return connectionCreationCount;
  }

  public LatencyStats getConnectionCreationStats() {
    return connectionCreationStats;
  }

  public long getStatementExecutionCount() {
    return statementExecutionCount;
  }

  public LatencyStats getStatementExecutionStats() {
    return statementExecutionStats;
  }

  /**
   * Generate a formatted string report.
   *
   * @param jdbcUrl the JDBC URL used in the test
   * @param concurrencyFactor the number of concurrent threads
   * @param totalTestTimeMs total test execution time in milliseconds
   * @param successfulThreads number of threads that completed successfully
   * @param failedThreads number of threads that failed
   * @return formatted report string
   */
  public String formatReport(
      String jdbcUrl,
      int concurrencyFactor,
      long totalTestTimeMs,
      int successfulThreads,
      int failedThreads) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("========================================\n");
    sb.append("CONCURRENT CONNECTION METRICS REPORT\n");
    sb.append("========================================\n");
    sb.append(String.format("Concurrency Factor: %d\n", concurrencyFactor));
    sb.append(String.format("JDBC URL: %s\n", maskUrl(jdbcUrl)));
    sb.append("\n");

    // FeatureFlags Endpoint Metrics
    sb.append("[1] FeatureFlags Endpoint Metrics\n");
    sb.append("-----------------------------------------\n");
    sb.append(String.format("Total Calls:           %d\n", featureFlagsCallCount));
    if (featureFlagsCallCount > 0) {
      sb.append(String.format("Min Latency:           %d ms\n", featureFlagsStats.getMin()));
      sb.append(String.format("Max Latency:           %d ms\n", featureFlagsStats.getMax()));
      sb.append(String.format("Avg Latency:           %.2f ms\n", featureFlagsStats.getAvg()));
      sb.append(String.format("P50 Latency:           %d ms\n", featureFlagsStats.getP50()));
      sb.append(String.format("P95 Latency:           %d ms\n", featureFlagsStats.getP95()));
      sb.append(String.format("P99 Latency:           %d ms\n", featureFlagsStats.getP99()));
    } else {
      sb.append("No FeatureFlags calls recorded.\n");
    }
    sb.append("\n");
    sb.append("Expected Calls:        ~1 (cached for same host)\n");
    if (featureFlagsCallCount <= 3) {
      sb.append("✓ PASS: Caching is working correctly\n");
    } else {
      sb.append("✗ FAIL: Too many FeatureFlags calls - caching may be broken!\n");
    }
    sb.append("\n");

    // Connection Creation Metrics
    sb.append("[2] Connection Creation Metrics\n");
    sb.append("-----------------------------------------\n");
    sb.append(String.format("Total Connections:     %d\n", connectionCreationCount));
    if (connectionCreationCount > 0) {
      sb.append(String.format("Min Latency:           %d ms\n", connectionCreationStats.getMin()));
      sb.append(String.format("Max Latency:           %d ms\n", connectionCreationStats.getMax()));
      sb.append(
          String.format("Avg Latency:           %.2f ms\n", connectionCreationStats.getAvg()));
      sb.append(String.format("P50 Latency:           %d ms\n", connectionCreationStats.getP50()));
      sb.append(String.format("P95 Latency:           %d ms\n", connectionCreationStats.getP95()));
      sb.append(String.format("P99 Latency:           %d ms\n", connectionCreationStats.getP99()));
    }
    sb.append("\n");

    // Statement Execution Metrics
    sb.append("[3] Statement Execution Metrics\n");
    sb.append("-----------------------------------------\n");
    sb.append(String.format("Total Executions:      %d\n", statementExecutionCount));
    if (statementExecutionCount > 0) {
      sb.append(String.format("Min Latency:           %d ms\n", statementExecutionStats.getMin()));
      sb.append(String.format("Max Latency:           %d ms\n", statementExecutionStats.getMax()));
      sb.append(
          String.format("Avg Latency:           %.2f ms\n", statementExecutionStats.getAvg()));
      sb.append(String.format("P50 Latency:           %d ms\n", statementExecutionStats.getP50()));
      sb.append(String.format("P95 Latency:           %d ms\n", statementExecutionStats.getP95()));
      sb.append(String.format("P99 Latency:           %d ms\n", statementExecutionStats.getP99()));
    }
    sb.append("\n");

    // Overall Test Execution
    sb.append("[4] Overall Test Execution\n");
    sb.append("-----------------------------------------\n");
    sb.append(
        String.format(
            "Total Test Time:       %d ms (%.2f sec)\n",
            totalTestTimeMs, totalTestTimeMs / 1000.0));
    sb.append(String.format("Successful Threads:    %d\n", successfulThreads));
    sb.append(String.format("Failed Threads:        %d\n", failedThreads));
    if (totalTestTimeMs > 0) {
      double throughput = (successfulThreads * 1000.0) / totalTestTimeMs;
      sb.append(String.format("Throughput:            %.2f connections/sec\n", throughput));
    }
    sb.append("========================================\n");

    return sb.toString();
  }

  private String maskUrl(String url) {
    // Mask the password/token in the URL
    return url.replaceAll("PWD=[^;]*", "PWD=****");
  }

  /** Statistics for a collection of latency measurements. */
  public static class LatencyStats {
    private final long min;
    private final long max;
    private final double avg;
    private final long p50;
    private final long p95;
    private final long p99;

    public LatencyStats(Queue<Long> latencies) {
      if (latencies == null || latencies.isEmpty()) {
        this.min = 0;
        this.max = 0;
        this.avg = 0.0;
        this.p50 = 0;
        this.p95 = 0;
        this.p99 = 0;
        return;
      }

      List<Long> sorted = new ArrayList<>(latencies);
      Collections.sort(sorted);

      this.min = sorted.get(0);
      this.max = sorted.get(sorted.size() - 1);
      this.avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
      this.p50 = percentile(sorted, 50);
      this.p95 = percentile(sorted, 95);
      this.p99 = percentile(sorted, 99);
    }

    private long percentile(List<Long> sorted, int percentile) {
      if (sorted.isEmpty()) {
        return 0;
      }
      int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
      index = Math.max(0, Math.min(index, sorted.size() - 1));
      return sorted.get(index);
    }

    public long getMin() {
      return min;
    }

    public long getMax() {
      return max;
    }

    public double getAvg() {
      return avg;
    }

    public long getP50() {
      return p50;
    }

    public long getP95() {
      return p95;
    }

    public long getP99() {
      return p99;
    }
  }
}
