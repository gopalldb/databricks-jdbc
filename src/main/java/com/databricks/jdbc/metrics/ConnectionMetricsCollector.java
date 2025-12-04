package com.databricks.jdbc.metrics;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe singleton to collect metrics during concurrent connection testing.
 *
 * <p>This collector tracks:
 *
 * <ul>
 *   <li>FeatureFlags endpoint call count and latencies
 *   <li>Connection creation count and latencies
 *   <li>Statement execution count and latencies
 * </ul>
 *
 * <p>Metrics collection must be explicitly enabled via {@link #enable()} before use.
 */
public class ConnectionMetricsCollector {
  private static final ConnectionMetricsCollector INSTANCE = new ConnectionMetricsCollector();

  private final AtomicBoolean enabled = new AtomicBoolean(false);

  // FeatureFlags endpoint metrics
  private final AtomicLong featureFlagsCallCount = new AtomicLong(0);
  private final ConcurrentLinkedQueue<Long> featureFlagsLatencies = new ConcurrentLinkedQueue<>();

  // Connection creation metrics
  private final AtomicLong connectionCreationCount = new AtomicLong(0);
  private final ConcurrentLinkedQueue<Long> connectionCreationLatencies =
      new ConcurrentLinkedQueue<>();

  // Statement execution metrics
  private final AtomicLong statementExecutionCount = new AtomicLong(0);
  private final ConcurrentLinkedQueue<Long> statementExecutionLatencies =
      new ConcurrentLinkedQueue<>();

  private ConnectionMetricsCollector() {
    // Private constructor for singleton
  }

  public static ConnectionMetricsCollector getInstance() {
    return INSTANCE;
  }

  /** Enable metrics collection. Must be called before any metrics are recorded. */
  public void enable() {
    enabled.set(true);
  }

  /** Disable metrics collection. */
  public void disable() {
    enabled.set(false);
  }

  /** Check if metrics collection is enabled. */
  public boolean isEnabled() {
    return enabled.get();
  }

  /** Reset all metrics to zero. Useful for running multiple test iterations. */
  public void reset() {
    featureFlagsCallCount.set(0);
    featureFlagsLatencies.clear();
    connectionCreationCount.set(0);
    connectionCreationLatencies.clear();
    statementExecutionCount.set(0);
    statementExecutionLatencies.clear();
  }

  /**
   * Record a FeatureFlags endpoint call.
   *
   * @param latencyMs the latency in milliseconds
   */
  public void recordFeatureFlagsCall(long latencyMs) {
    if (enabled.get()) {
      featureFlagsCallCount.incrementAndGet();
      featureFlagsLatencies.add(latencyMs);
    }
  }

  /**
   * Record a connection creation.
   *
   * @param latencyMs the latency in milliseconds
   */
  public void recordConnectionCreation(long latencyMs) {
    if (enabled.get()) {
      connectionCreationCount.incrementAndGet();
      connectionCreationLatencies.add(latencyMs);
    }
  }

  /**
   * Record a statement execution.
   *
   * @param latencyMs the latency in milliseconds
   */
  public void recordStatementExecution(long latencyMs) {
    if (enabled.get()) {
      statementExecutionCount.incrementAndGet();
      statementExecutionLatencies.add(latencyMs);
    }
  }

  /**
   * Get a comprehensive metrics report.
   *
   * @return MetricsReport containing all collected metrics
   */
  public MetricsReport getMetricsReport() {
    return new MetricsReport(
        featureFlagsCallCount.get(),
        featureFlagsLatencies,
        connectionCreationCount.get(),
        connectionCreationLatencies,
        statementExecutionCount.get(),
        statementExecutionLatencies);
  }
}
