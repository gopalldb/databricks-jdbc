package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.telemetry.TelemetryHelper.DEFAULT_HOST;
import static com.databricks.jdbc.telemetry.TelemetryHelper.isTelemetryAllowedForConnection;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TelemetryClientFactory {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(TelemetryClientFactory.class);

  private static final TelemetryClientFactory INSTANCE = new TelemetryClientFactory();

  @VisibleForTesting
  final Map<String, TelemetryClientHolder> telemetryClientHolders = new ConcurrentHashMap<>();

  @VisibleForTesting
  final Map<String, TelemetryClientHolder> noauthTelemetryClientHolders = new ConcurrentHashMap<>();

  /**
   * Tracks connection UUIDs that have been closed. When a connection is closed, its UUID is added
   * here so that subsequent getTelemetryClient() calls (e.g., from delayed flush tasks or collector
   * exports) return NoopTelemetryClient instead of re-creating an orphaned TelemetryClient. This
   * prevents the socket leak described in GitHub issue #1325.
   */
  @VisibleForTesting final Set<String> closedConnectionUuids = ConcurrentHashMap.newKeySet();

  private final ExecutorService telemetryExecutorService;
  private ScheduledExecutorService sharedSchedulerService;

  private static ThreadFactory createThreadFactory() {
    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Telemetry-Thread-" + threadNumber.getAndIncrement());
        // TODO : https://databricks.atlassian.net/browse/PECO-2716
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  private static ThreadFactory createSchedulerThreadFactory() {
    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Telemetry-Scheduler-" + threadNumber.getAndIncrement());
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  private TelemetryClientFactory() {
    telemetryExecutorService = Executors.newFixedThreadPool(10, createThreadFactory());
    sharedSchedulerService =
        Executors.newSingleThreadScheduledExecutor(createSchedulerThreadFactory());
  }

  public static TelemetryClientFactory getInstance() {
    return INSTANCE;
  }

  public ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
    if (!isTelemetryAllowedForConnection(connectionContext)) {
      return NoopTelemetryClient.getInstance();
    }
    // Prevent re-creation of TelemetryClient for connections that have been closed.
    // Without this guard, code paths that call getTelemetryClient() after
    // closeTelemetryClient() (e.g., TelemetryCollector.exportAllPendingTelemetryDetails
    // or delayed TelemetryPushTask flush) would create an orphaned TelemetryClient
    // whose periodic flush creates leaked TELEMETRY HTTP clients (issue #1325).
    if (closedConnectionUuids.contains(connectionContext.getConnectionUuid())) {
      return NoopTelemetryClient.getInstance();
    }
    DatabricksConfig databricksConfig =
        TelemetryHelper.getDatabricksConfigSafely(connectionContext);
    if (databricksConfig != null) {
      String key = TelemetryHelper.keyOf(connectionContext);
      TelemetryClientHolder holder =
          telemetryClientHolders.compute(
              key,
              (k, existing) -> {
                if (existing == null) {
                  try {
                    return new TelemetryClientHolder(
                        new TelemetryClient(
                            connectionContext,
                            getTelemetryExecutorService(),
                            getSharedSchedulerService(),
                            databricksConfig),
                        connectionContext.getConnectionUuid());
                  } catch (Exception e) {
                    // Validation or other errors during client creation - fail silently
                    LOGGER.trace("Skipping telemetry, client creation failed {}", e);
                    return null;
                  }
                }
                // Track this unique connection
                existing.connectionUuids.add(connectionContext.getConnectionUuid());
                return existing;
              });
      return holder != null ? holder.client : NoopTelemetryClient.getInstance();
    }
    // Use no-auth telemetry client if connection creation failed.
    String key = TelemetryHelper.keyOf(connectionContext);
    TelemetryClientHolder holder =
        noauthTelemetryClientHolders.compute(
            key,
            (k, existing) -> {
              if (existing == null) {
                try {
                  return new TelemetryClientHolder(
                      new TelemetryClient(
                          connectionContext,
                          getTelemetryExecutorService(),
                          getSharedSchedulerService()),
                      connectionContext.getConnectionUuid());
                } catch (Exception e) {
                  // Validation or other errors during client creation - fail silently
                  LOGGER.trace("Skipping no-auth telemetry, client creation failed {}", e);
                  return null;
                }
              }
              // Track this unique connection
              existing.connectionUuids.add(connectionContext.getConnectionUuid());
              return existing;
            });
    return holder != null ? holder.client : NoopTelemetryClient.getInstance();
  }

  /**
   * Closes telemetry client for a connection. Thread-safe: computeIfPresent ensures atomic locking,
   * preventing race conditions between connection removal and addition.
   *
   * <p>The connection UUID is added to closedConnectionUuids FIRST to prevent getTelemetryClient()
   * from re-creating a TelemetryClient during or after the close sequence. Pending
   * TelemetryCollector events are exported BEFORE the TelemetryClient is closed, so they are
   * flushed through the existing client. See GitHub issue #1325.
   */
  public void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
    String key = TelemetryHelper.keyOf(connectionContext);
    String connectionUuid = connectionContext.getConnectionUuid();

    // Export pending TelemetryCollector events BEFORE closing the TelemetryClient.
    // This ensures the export uses the existing TelemetryClient (via the holder map)
    // rather than triggering re-creation after the holder is removed.
    TelemetryCollector collector =
        TelemetryCollectorManager.getInstance().removeCollector(connectionContext);
    if (collector != null) {
      collector.exportAllPendingTelemetryDetails();
    }

    // Mark the connection as closed to prevent getTelemetryClient() from re-creating a
    // TelemetryClient if called by delayed flush tasks or collector exports (issue #1325).
    // This is done inside computeIfPresent so it only applies to connections that actually
    // had a telemetry client registered.
    telemetryClientHolders.computeIfPresent(
        key,
        (k, holder) -> {
          holder.connectionUuids.remove(connectionUuid);
          closedConnectionUuids.add(connectionUuid);
          if (holder.connectionUuids.isEmpty()) {
            closeTelemetryClient(holder.client, "telemetry client");
            return null;
          }
          return holder;
        });
    noauthTelemetryClientHolders.computeIfPresent(
        key,
        (k, holder) -> {
          holder.connectionUuids.remove(connectionUuid);
          closedConnectionUuids.add(connectionUuid);
          if (holder.connectionUuids.isEmpty()) {
            closeTelemetryClient(holder.client, "unauthenticated telemetry client");
            return null;
          }
          return holder;
        });

    // Clean up cached connection parameters to prevent memory leaks
    TelemetryHelper.removeConnectionParameters(connectionContext.getConnectionUuid());
  }

  public ExecutorService getTelemetryExecutorService() {
    return telemetryExecutorService;
  }

  public ScheduledExecutorService getSharedSchedulerService() {
    return sharedSchedulerService;
  }

  static ITelemetryPushClient getTelemetryPushClient(
      Boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext,
      DatabricksConfig databricksConfig) {
    ITelemetryPushClient pushClient =
        new TelemetryPushClient(isAuthenticated, connectionContext, databricksConfig);
    if (connectionContext.isTelemetryCircuitBreakerEnabled()) {
      // If circuit breaker is enabled, use the circuit breaker client
      String host = null;
      try {
        host = connectionContext.getHostUrl();
      } catch (DatabricksParsingException e) {
        // Even though Telemetry logs should be trace or debug, we are treating this as error,
        // since host parsing is fundamental to JDBC.
        LOGGER.error(e, "Error parsing host url");
        // Fallback to a default value, we don't want to throw any exception from Telemetry
        host = DEFAULT_HOST;
      }
      pushClient = new CircuitBreakerTelemetryPushClient(pushClient, host);
    }
    return pushClient;
  }

  @VisibleForTesting
  public void reset() {
    // Close all existing clients (cancels their scheduled tasks)
    telemetryClientHolders.values().forEach(holder -> holder.client.close());
    noauthTelemetryClientHolders.values().forEach(holder -> holder.client.close());

    // Clear the maps
    telemetryClientHolders.clear();
    noauthTelemetryClientHolders.clear();
    closedConnectionUuids.clear();

    // Clear cached connection parameters
    TelemetryHelper.clearConnectionParameterCache();

    // Shutdown shared scheduler service (test cleanup only)
    sharedSchedulerService.shutdown();
    try {
      if (!sharedSchedulerService.awaitTermination(5, TimeUnit.SECONDS)) {
        sharedSchedulerService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      sharedSchedulerService.shutdownNow();
    }

    // Recreate the scheduler for subsequent tests
    sharedSchedulerService =
        Executors.newSingleThreadScheduledExecutor(createSchedulerThreadFactory());
  }

  private void closeTelemetryClient(ITelemetryClient client, String clientType) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.debug("Caught error while closing {}. Error: {}", clientType, e);
      }
    }
  }

  private static final class TelemetryClientHolder {
    final TelemetryClient client;
    final Set<String> connectionUuids; // Track unique connections

    TelemetryClientHolder(TelemetryClient client, String connectionUuid) {
      this.client = client;
      this.connectionUuids = ConcurrentHashMap.newKeySet();
      this.connectionUuids.add(connectionUuid);
    }
  }

  private static String keyOf(IDatabricksConnectionContext context) {
    String host = context.getHostForOAuth();
    return host != null ? host : DEFAULT_HOST;
  }
}
