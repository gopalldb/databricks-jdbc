package com.databricks.jdbc.dbclient.impl.http;

import static java.util.AbstractMap.SimpleEntry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksHttpClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpClientFactory.class);
  private static final DatabricksHttpClientFactory INSTANCE = new DatabricksHttpClientFactory();
  private final ConcurrentHashMap<SimpleEntry<String, HttpClientType>, DatabricksHttpClient>
      instances = new ConcurrentHashMap<>();

  /**
   * Tracks connection UUIDs for which removeClient() has been called. Prevents getClient() from
   * re-creating HTTP clients for closed connections via computeIfAbsent. Without this guard,
   * delayed TelemetryPushTask executions can create orphaned HTTP clients that leak TCP sockets.
   * See GitHub issue #1325.
   */
  private final Set<String> closedConnections = ConcurrentHashMap.newKeySet();

  private DatabricksHttpClientFactory() {
    // Private constructor to prevent instantiation
  }

  public static DatabricksHttpClientFactory getInstance() {
    return INSTANCE;
  }

  public IDatabricksHttpClient getClient(IDatabricksConnectionContext context) {
    return getClient(context, HttpClientType.COMMON);
  }

  public IDatabricksHttpClient getClient(
      IDatabricksConnectionContext context, HttpClientType type) {
    // Prevent creating new HTTP clients for connections that have been closed.
    // This guards against delayed TelemetryPushTask executions that call
    // getClient(ctx, TELEMETRY) after removeClient(ctx) has already run.
    String connectionUuid = context.getConnectionUuid();
    if (connectionUuid != null && closedConnections.contains(connectionUuid)) {
      LOGGER.debug(
          "Rejecting getClient() for closed connection {} with type {}",
          context.getConnectionUuid(),
          type);
      return null;
    }
    return instances.computeIfAbsent(
        getClientKey(context.getConnectionUuid(), type),
        k -> new DatabricksHttpClient(context, type));
  }

  /**
   * Removes and closes all HTTP clients for the given connection. Does NOT mark the connection as
   * closed — the client can be re-created by a subsequent getClient() call.
   */
  public void removeClient(IDatabricksConnectionContext context) {
    for (HttpClientType type : HttpClientType.values()) {
      removeClient(context, type);
    }
  }

  /**
   * Permanently closes all HTTP clients for the given connection and prevents new ones from being
   * created. This should be called from DatabricksConnection.close() to prevent delayed
   * TelemetryPushTask executions from creating orphaned HTTP clients (issue #1325).
   */
  public void closeConnection(IDatabricksConnectionContext context) {
    closedConnections.add(context.getConnectionUuid());
    removeClient(context);
  }

  public void removeClient(IDatabricksConnectionContext context, HttpClientType type) {
    DatabricksHttpClient instance =
        instances.remove(getClientKey(context.getConnectionUuid(), type));
    if (instance != null) {
      try {
        instance.close();
      } catch (IOException e) {
        LOGGER.debug("Caught error while closing http client. Error {}", e);
      }
    }
  }

  private SimpleEntry<String, HttpClientType> getClientKey(String uuid, HttpClientType clientType) {
    return new SimpleEntry<>(uuid, clientType);
  }
}
