package com.databricks.jdbc.dbclient.impl.http;

import static java.util.AbstractMap.SimpleEntry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
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
   * Tracks connection UUIDs that are currently open. When a connection closes, its UUID is removed
   * from this set. The {@link #getClient} method checks this set inside the {@code computeIfAbsent}
   * lambda to atomically reject client creation for closed connections.
   *
   * <p>This allowlist approach (vs. a denylist of closed UUIDs) ensures the set naturally shrinks
   * as connections close, avoiding unbounded heap growth in long-running JVMs (e.g., CRaC). See
   * GitHub issue #1325.
   */
  private final Set<String> openConnections = ConcurrentHashMap.newKeySet();

  private DatabricksHttpClientFactory() {
    // Private constructor to prevent instantiation
  }

  public static DatabricksHttpClientFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Registers a connection UUID as open. Must be called early in the connection lifecycle (before
   * any {@link #getClient} call) so that the allowlist guard permits client creation.
   */
  public void registerConnection(String connectionUuid) {
    if (connectionUuid != null) {
      openConnections.add(connectionUuid);
    }
  }

  public IDatabricksHttpClient getClient(IDatabricksConnectionContext context) {
    return getClient(context, HttpClientType.COMMON);
  }

  /**
   * Returns an HTTP client for the given connection and type, creating one if needed. Returns null
   * if the connection has been closed (UUID not in the open set). The closed-connection guard is
   * inside the {@code computeIfAbsent} lambda to prevent the TOCTOU race between checking the guard
   * and creating the client (issue #1325).
   */
  public IDatabricksHttpClient getClient(
      IDatabricksConnectionContext context, HttpClientType type) {
    String connectionUuid = context.getConnectionUuid();
    return instances.computeIfAbsent(
        getClientKey(connectionUuid, type),
        k -> {
          // Guard: reject creation for closed (or never-registered) connections.
          if (connectionUuid != null && !openConnections.contains(connectionUuid)) {
            LOGGER.debug(
                "Rejecting getClient() for closed connection {} with type {}",
                connectionUuid,
                type);
            return null;
          }
          return new DatabricksHttpClient(context, type);
        });
  }

  /**
   * Permanently closes all HTTP clients for the given connection and prevents new ones from being
   * created. Called from {@link com.databricks.jdbc.api.impl.DatabricksConnection#close()}.
   */
  public void closeConnection(IDatabricksConnectionContext context) {
    String uuid = context.getConnectionUuid();
    if (uuid != null) {
      openConnections.remove(uuid);
    }
    removeClient(context);
  }

  /**
   * Removes and closes all HTTP clients for the given connection. Does NOT affect the open
   * connections set — the client can be re-created by a subsequent getClient() call.
   */
  @VisibleForTesting
  public void removeClient(IDatabricksConnectionContext context) {
    for (HttpClientType type : HttpClientType.values()) {
      removeClient(context, type);
    }
  }

  @VisibleForTesting
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

  /** Resets all state. For test cleanup only. */
  @VisibleForTesting
  public void reset() {
    instances
        .values()
        .forEach(
            client -> {
              try {
                client.close();
              } catch (IOException e) {
                LOGGER.debug("Error closing HTTP client during reset: {}", e);
              }
            });
    instances.clear();
    openConnections.clear();
  }

  private SimpleEntry<String, HttpClientType> getClientKey(String uuid, HttpClientType clientType) {
    return new SimpleEntry<>(uuid, clientType);
  }
}
