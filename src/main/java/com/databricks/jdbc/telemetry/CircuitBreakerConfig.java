package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.time.Duration;

/**
 * Configuration for the telemetry circuit breaker. This class provides configurable parameters for
 * the circuit breaker pattern.
 */
public class CircuitBreakerConfig {

  private final float failureRateThreshold;
  private final int minimumNumberOfCalls;
  private final int slidingWindowSize;
  private final Duration waitDurationInOpenState;
  private final int permittedNumberOfCallsInHalfOpenState;

  private CircuitBreakerConfig(IDatabricksConnectionContext connectionContext) {
    this.failureRateThreshold = connectionContext.getTelemetryCircuitBreakerFailureRateThreshold();
    this.minimumNumberOfCalls = connectionContext.getTelemetryCircuitBreakerMinimumNumberOfCalls();
    this.slidingWindowSize = connectionContext.getTelemetryCircuitBreakerSlidingWindowSize();
    this.waitDurationInOpenState =
        Duration.ofSeconds(
            connectionContext.getTelemetryCircuitBreakerWaitDurationSecondsInOpenState());
    this.permittedNumberOfCallsInHalfOpenState =
        connectionContext.getTelemetryCircuitBreakerPermittedNumberOfCallsInHalfOpenState();
  }

  public float getFailureRateThreshold() {
    return failureRateThreshold;
  }

  public int getMinimumNumberOfCalls() {
    return minimumNumberOfCalls;
  }

  public int getSlidingWindowSize() {
    return slidingWindowSize;
  }

  public Duration getWaitDurationInOpenState() {
    return waitDurationInOpenState;
  }

  public int getPermittedNumberOfCallsInHalfOpenState() {
    return permittedNumberOfCallsInHalfOpenState;
  }
}
