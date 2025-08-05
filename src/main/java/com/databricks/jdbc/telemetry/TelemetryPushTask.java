package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class TelemetryPushTask implements Runnable {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryPushTask.class);

  private final List<TelemetryFrontendLog> queueToBePushed;
  private final boolean isAuthenticated;
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final ObjectMapper objectMapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  TelemetryPushTask(
      List<TelemetryFrontendLog> eventsQueue,
      boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext,
      DatabricksConfig databricksConfig) {
    this.queueToBePushed = eventsQueue;
    this.isAuthenticated = isAuthenticated;
    this.connectionContext = connectionContext;
    this.databricksConfig = databricksConfig;
  }

  @Override
  public void run() {
    LOGGER.debug("Pushing Telemetry logs of size {}", queueToBePushed.size());
    TelemetryRequest request = new TelemetryRequest();
    if (queueToBePushed.isEmpty()) {
      return;
    }
    try {
      request
          .setUploadTime(System.currentTimeMillis())
          .setProtoLogs(
              queueToBePushed.stream()
                  .map(
                      event -> {
                        try {
                          return objectMapper.writeValueAsString(event);
                        } catch (JsonProcessingException e) {
                          LOGGER.error(
                              "Failed to serialize Telemetry event {} with error: {}", event, e);
                          return null; // Return null for failed serialization
                        }
                      })
                  .filter(Objects::nonNull) // Remove nulls from failed serialization
                  .collect(Collectors.toList()));

      ITelemetryPushClient pushClient =
          new TelemetryPushClient(isAuthenticated, connectionContext, databricksConfig);
      if (connectionContext.isTelemetryCircuitBreakerEnabled()) {
        // If circuit breaker is enabled, use the circuit breaker client
        pushClient =
            new CircuitBreakerTelemetryPushClient(pushClient, connectionContext.getHostUrl());
      }
      pushClient.pushEvent(request);
    } catch (Exception e) {
      // Retry is already handled in HTTP client, we can return from here
      LOGGER.trace("Failed to push telemetry logs because of the error {}", e.getMessage());
    }
  }
}
