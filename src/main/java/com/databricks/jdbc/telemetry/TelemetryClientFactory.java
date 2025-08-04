package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.telemetry.TelemetryHelper.isTelemetryAllowedForConnection;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;


public class TelemetryClientFactory {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(TelemetryClientFactory.class);

  private static final TelemetryClientFactory INSTANCE = new TelemetryClientFactory();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> telemetryClients = new LinkedHashMap<>();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> noauthTelemetryClients = new LinkedHashMap<>();

  private final ExecutorService telemetryExecutorService;

  private TelemetryClientFactory() {
    telemetryExecutorService = Executors.newFixedThreadPool(10);
  }

  public static TelemetryClientFactory getInstance() {
    return INSTANCE;
  }

  public ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
    if (!isTelemetryAllowedForConnection(connectionContext)) {
      return NoopTelemetryClient.getInstance();
    }
    DatabricksConfig databricksConfig =
        TelemetryHelper.getDatabricksConfigSafely(connectionContext);
    if (databricksConfig != null) {
      return telemetryClients.computeIfAbsent(
          connectionContext.getConnectionUuid(),
          k -> {
              TelemetryClient baseClient = new TelemetryClient(
                  connectionContext, getTelemetryExecutorService(), databricksConfig);
              CircuitBreakerConfig config = CircuitBreakerConfigurator.createConfig(connectionContext);
              return config != null ? new CircuitBreakerTelemetryClient(baseClient, config) : baseClient;
          });
    }
    // Use no-auth telemetry client if connection creation failed.
    return noauthTelemetryClients.computeIfAbsent(
        connectionContext.getConnectionUuid(),
        k -> {
            TelemetryClient baseClient = new TelemetryClient(
                connectionContext, getTelemetryExecutorService());
            CircuitBreakerConfig config = CircuitBreakerConfigurator.createConfig(connectionContext);
            return config != null ? new CircuitBreakerTelemetryClient(baseClient, config) : baseClient;
        });
  }

  public void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
    closeTelemetryClient(
        telemetryClients.remove(connectionContext.getConnectionUuid()), "telemetry client");
    closeTelemetryClient(
        noauthTelemetryClients.remove(connectionContext.getConnectionUuid()),
        "unauthenticated telemetry client");
  }

  public ExecutorService getTelemetryExecutorService() {
    return telemetryExecutorService;
  }

  @VisibleForTesting
  public void reset() {
    // Close all existing clients
    telemetryClients.values().forEach(TelemetryClient::close);
    noauthTelemetryClients.values().forEach(TelemetryClient::close);

    // Clear the maps
    telemetryClients.clear();
    noauthTelemetryClients.clear();
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
}

