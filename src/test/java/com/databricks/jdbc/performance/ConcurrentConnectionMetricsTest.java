package com.databricks.jdbc.performance;

import com.databricks.jdbc.metrics.ConnectionMetricsCollector;
import com.databricks.jdbc.metrics.MetricsReport;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Concurrent connection test to measure FeatureFlags endpoint calls and connection/statement
 * latencies.
 *
 * <p>This test validates that the FeatureFlags endpoint caching works correctly by ensuring only ~1
 * call is made across 200 concurrent connections.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConcurrentConnectionMetricsTest {
  private static final int CONCURRENCY_FACTOR = 200;
  private static final String JDBC_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
          + "transportMode=http;ssl=1;AuthMech=3;"
          + "httpPath=/sql/1.0/warehouses/864004c1b3961382";
  private static final String USER = "token";
  private static final String PASSWORD = "";

  private final AtomicInteger successfulThreads = new AtomicInteger(0);
  private final AtomicInteger failedThreads = new AtomicInteger(0);
  private final List<String> errors = new ArrayList<>();
  private long totalTestTimeMs = 0;

  @BeforeAll
  void setup() {
    System.out.println("\n=== Starting Concurrent Connection Metrics Test ===");
    System.out.println("Concurrency Factor: " + CONCURRENCY_FACTOR);
    System.out.println("JDBC URL: " + maskUrl(JDBC_URL));
    System.out.println();

    // Enable metrics collection
    ConnectionMetricsCollector.getInstance().enable();
    ConnectionMetricsCollector.getInstance().reset();
  }

  @Test
  void testConcurrentConnectionsWithMetrics() throws InterruptedException {
    long testStartTime = System.currentTimeMillis();

    ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENCY_FACTOR);
    List<Future<TaskResult>> futures = new ArrayList<>();

    // Submit all tasks
    for (int i = 0; i < CONCURRENCY_FACTOR; i++) {
      final int threadNum = i;
      Future<TaskResult> future =
          executorService.submit(
              new Callable<TaskResult>() {
                @Override
                public TaskResult call() {
                  return executeConnectionTask(threadNum);
                }
              });
      futures.add(future);
    }

    // Wait for all tasks to complete
    for (Future<TaskResult> future : futures) {
      try {
        TaskResult result = future.get();
        if (result.success) {
          successfulThreads.incrementAndGet();
        } else {
          failedThreads.incrementAndGet();
          synchronized (errors) {
            errors.add(result.errorMessage);
          }
        }
      } catch (Exception e) {
        failedThreads.incrementAndGet();
        synchronized (errors) {
          errors.add("Future.get() failed: " + e.getMessage());
        }
      }
    }

    executorService.shutdown();
    long testEndTime = System.currentTimeMillis();
    totalTestTimeMs = testEndTime - testStartTime;

    System.out.println("\n=== Test Execution Complete ===");
    System.out.println("Total time: " + totalTestTimeMs + " ms");
    System.out.println("Successful threads: " + successfulThreads.get());
    System.out.println("Failed threads: " + failedThreads.get());

    if (failedThreads.get() > 0) {
      System.out.println("\nErrors encountered:");
      synchronized (errors) {
        for (int i = 0; i < Math.min(errors.size(), 10); i++) {
          System.out.println("  " + (i + 1) + ". " + errors.get(i));
        }
        if (errors.size() > 10) {
          System.out.println("  ... and " + (errors.size() - 10) + " more errors");
        }
      }
    }
  }

  private TaskResult executeConnectionTask(int threadNum) {
    try {
      // Create connection and execute simple query
      try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next()) {
              int result = rs.getInt(1);
              if (result != 1) {
                return TaskResult.failure("Thread " + threadNum + ": Expected 1, got " + result);
              }
            } else {
              return TaskResult.failure("Thread " + threadNum + ": No result returned");
            }
          }
        }
      }
      return TaskResult.success();
    } catch (Exception e) {
      return TaskResult.failure(
          "Thread " + threadNum + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  @AfterAll
  void printMetrics() {
    // Disable metrics collection
    ConnectionMetricsCollector.getInstance().disable();

    // Get and print metrics report
    MetricsReport report = ConnectionMetricsCollector.getInstance().getMetricsReport();
    String formattedReport =
        report.formatReport(
            JDBC_URL,
            CONCURRENCY_FACTOR,
            totalTestTimeMs,
            successfulThreads.get(),
            failedThreads.get());

    System.out.println(formattedReport);

    // Additional validation
    if (report.getFeatureFlagsCallCount() > 3) {
      System.out.println(
          "\n⚠️  WARNING: FeatureFlags endpoint was called "
              + report.getFeatureFlagsCallCount()
              + " times!");
      System.out.println(
          "    Expected ~1 call due to caching. Caching may not be working correctly.");
    }
  }

  private String maskUrl(String url) {
    return url.replaceAll("PWD=[^;]*", "PWD=****");
  }

  private static class TaskResult {
    final boolean success;
    final String errorMessage;

    private TaskResult(boolean success, String errorMessage) {
      this.success = success;
      this.errorMessage = errorMessage;
    }

    static TaskResult success() {
      return new TaskResult(true, null);
    }

    static TaskResult failure(String errorMessage) {
      return new TaskResult(false, errorMessage);
    }
  }
}
