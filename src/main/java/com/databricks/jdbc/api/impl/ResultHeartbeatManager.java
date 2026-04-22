package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Manages periodic heartbeat tasks to keep server-side result state alive while the client consumes
 * results slowly. One instance per connection, shared across all statements.
 *
 * <p>Each active result set can register a heartbeat task that periodically calls
 * GetStatementStatus (SEA) or GetOperationStatus (Thrift) to signal the server that the client is
 * still consuming results. This prevents premature operation expiry and warehouse auto-stop.
 *
 * <p>Heartbeats are stopped when:
 *
 * <ul>
 *   <li>All results are consumed (ResultSet.next() returns false)
 *   <li>ResultSet.close() is called
 *   <li>Statement.close() is called (safety net)
 *   <li>Connection.close() calls shutdown()
 *   <li>The heartbeat task itself detects a terminal state or max consecutive failures
 * </ul>
 */
class ResultHeartbeatManager {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ResultHeartbeatManager.class);

  private final ScheduledExecutorService scheduler;
  private final Map<StatementId, ScheduledFuture<?>> activeHeartbeats = new ConcurrentHashMap<>();
  private final int intervalSeconds;
  private volatile boolean isShutdown = false;

  ResultHeartbeatManager(int intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "databricks-jdbc-heartbeat");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Starts a periodic heartbeat for the given statement. The task runs every {@code
   * intervalSeconds} after an initial delay equal to the interval.
   *
   * @param statementId the statement to keep alive
   * @param heartbeatTask the task that sends the heartbeat RPC. Must handle its own exceptions.
   */
  void startHeartbeat(StatementId statementId, Runnable heartbeatTask) {
    if (isShutdown || statementId == null) {
      return;
    }

    // Stop any existing heartbeat for this statement (e.g., re-execution)
    stopHeartbeat(statementId);

    LOGGER.debug(
        "Starting heartbeat for statement {} with interval {}s", statementId, intervalSeconds);

    ScheduledFuture<?> future =
        scheduler.scheduleAtFixedRate(
            heartbeatTask, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    activeHeartbeats.put(statementId, future);
  }

  /**
   * Stops the heartbeat for the given statement. Idempotent — safe to call multiple times or for
   * statements that have no active heartbeat.
   */
  void stopHeartbeat(StatementId statementId) {
    if (statementId == null) {
      return;
    }

    ScheduledFuture<?> future = activeHeartbeats.remove(statementId);
    if (future != null) {
      future.cancel(false); // don't interrupt if currently running
      LOGGER.debug("Stopped heartbeat for statement {}", statementId);
    }
  }

  /** Stops all heartbeats and shuts down the scheduler. Called on Connection.close(). */
  void shutdown() {
    isShutdown = true;

    for (Map.Entry<StatementId, ScheduledFuture<?>> entry : activeHeartbeats.entrySet()) {
      entry.getValue().cancel(false);
      LOGGER.debug("Stopped heartbeat for statement {} during shutdown", entry.getKey());
    }
    activeHeartbeats.clear();

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }

    LOGGER.debug("Heartbeat manager shut down");
  }

  /** Returns the number of active heartbeats. Visible for testing. */
  int getActiveHeartbeatCount() {
    return activeHeartbeats.size();
  }

  boolean isShutdown() {
    return isShutdown;
  }
}
