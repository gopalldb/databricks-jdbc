package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.time.Duration;

/** Utility class to configure circuit breaker settings based on connection context. */
public class CircuitBreakerConfigurator {

  private static final String TELEMETRY_CIRCUIT_BREAKER_ENABLED =
      "telemetry.circuit.breaker.enabled";
  private static final String TELEMETRY_CIRCUIT_BREAKER_FAILURE_RATE =
      "telemetry.circuit.breaker.failure.rate";
  private static final String TELEMETRY_CIRCUIT_BREAKER_MIN_CALLS =
      "telemetry.circuit.breaker.min.calls";
  private static final String TELEMETRY_CIRCUIT_BREAKER_WINDOW_SIZE =
      "telemetry.circuit.breaker.window.size";
  private static final String TELEMETRY_CIRCUIT_BREAKER_WAIT_DURATION =
      "telemetry.circuit.breaker.wait.duration";
  private static final String TELEMETRY_CIRCUIT_BREAKER_HALF_OPEN_CALLS =
      "telemetry.circuit.breaker.half.open.calls";

  /**
   * Creates a circuit breaker configuration based on connection context properties.
   *
   * @param connectionContext The connection context containing configuration properties
   * @return CircuitBreakerConfig with settings from connection context or defaults
   */
  public static CircuitBreakerConfig createConfig(IDatabricksConnectionContext connectionContext) {
    CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.builder();

    // Check if circuit breaker is enabled (default: true)
    boolean enabled =
        getBooleanProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_ENABLED, true);
    if (!enabled) {
      return null; // Return null to indicate circuit breaker should not be used
    }

    // Configure failure rate threshold (default: 50.0)
    float failureRate =
        getFloatProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_FAILURE_RATE, 50.0f);
    builder.failureRateThreshold(failureRate);

    // Configure minimum number of calls (default: 10)
    int minCalls = getIntProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_MIN_CALLS, 10);
    builder.minimumNumberOfCalls(minCalls);

    // Configure sliding window size (default: 20)
    int windowSize = getIntProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_WINDOW_SIZE, 20);
    builder.slidingWindowSize(windowSize);

    // Configure wait duration in open state (default: 1 minute)
    int waitDurationSeconds =
        getIntProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_WAIT_DURATION, 60);
    builder.waitDurationInOpenState(Duration.ofSeconds(waitDurationSeconds));

    // Configure permitted calls in half-open state (default: 5)
    int halfOpenCalls =
        getIntProperty(connectionContext, TELEMETRY_CIRCUIT_BREAKER_HALF_OPEN_CALLS, 5);
    builder.permittedNumberOfCallsInHalfOpenState(halfOpenCalls);

    return builder.build();
  }

  private static boolean getBooleanProperty(
      IDatabricksConnectionContext context, String key, boolean defaultValue) {
    String value = context.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  private static float getFloatProperty(
      IDatabricksConnectionContext context, String key, float defaultValue) {
    String value = context.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static int getIntProperty(
      IDatabricksConnectionContext context, String key, int defaultValue) {
    String value = context.getProperty(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
