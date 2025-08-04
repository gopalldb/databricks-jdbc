package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TelemetryClient wrapper that implements circuit breaker pattern using Resilience4j. This wrapper
 * handles server unavailability and resource exhausted errors by temporarily stopping telemetry
 * requests when the service is experiencing issues.
 */
public class CircuitBreakerTelemetryClient implements ITelemetryClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CircuitBreakerTelemetryClient.class);

  private final ITelemetryClient delegate;
  private final CircuitBreaker circuitBreaker;
  private final AtomicReference<CircuitBreaker.State> lastState = new AtomicReference<>();

  public CircuitBreakerTelemetryClient(ITelemetryClient delegate) {
    this(delegate, CircuitBreakerConfig.builder().build());
  }

  public CircuitBreakerTelemetryClient(ITelemetryClient delegate, CircuitBreakerConfig config) {
    this.delegate = delegate;
    this.circuitBreaker = createCircuitBreaker(config);

    // Add state change listener for logging
    circuitBreaker
        .getEventPublisher()
        .onStateTransition(
            event -> {
              CircuitBreaker.State newState = event.getStateTransition().getToState();
              CircuitBreaker.State oldState = lastState.getAndSet(newState);

              if (oldState != newState) {
                LOGGER.info(
                    "Telemetry circuit breaker state changed from {} to {}", oldState, newState);
              }
            });
  }

  private CircuitBreaker createCircuitBreaker(CircuitBreakerConfig config) {
    CircuitBreakerConfig.Builder resilience4jConfig =
        io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
            // Failure rate threshold - circuit opens when configured percentage of calls fail
            .failureRateThreshold(config.getFailureRateThreshold())
            // Minimum number of calls before circuit can open
            .minimumNumberOfCalls(config.getMinimumNumberOfCalls())
            // Sliding window size for failure rate calculation
            .slidingWindowSize(config.getSlidingWindowSize())
            // Wait duration before transitioning from OPEN to HALF_OPEN
            .waitDurationInOpenState(config.getWaitDurationInOpenState())
            // Number of calls in HALF_OPEN state before deciding to close or open
            .permittedNumberOfCallsInHalfOpenState(
                config.getPermittedNumberOfCallsInHalfOpenState())
            // Exceptions that should be treated as failures
            .recordExceptions(
                // Server unavailability errors
                java.net.ConnectException.class,
                java.net.SocketTimeoutException.class,
                java.net.NoRouteToHostException.class,
                java.net.UnknownHostException.class,
                // Resource exhausted errors
                java.util.concurrent.RejectedExecutionException.class,
                java.lang.OutOfMemoryError.class,
                // HTTP errors that indicate server issues
                org.apache.http.client.HttpResponseException.class,
                // Databricks specific exceptions
                com.databricks.jdbc.exception.DatabricksHttpException.class)
            // Exceptions that should be ignored (not counted as failures)
            .ignoreExceptions(
                // Client-side errors that shouldn't trigger circuit breaker
                IllegalArgumentException.class, NullPointerException.class)
            .build();

    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(resilience4jConfig);
    return registry.circuitBreaker("telemetry-client");
  }

  @Override
  public void exportEvent(TelemetryFrontendLog event) {
    try {
      // Execute the telemetry export with circuit breaker protection
      circuitBreaker.executeRunnable(() -> delegate.exportEvent(event));
    } catch (Exception e) {
      // Log the error but don't re-throw to prevent application disruption
      LOGGER.debug("Circuit breaker prevented telemetry export due to error: {}", e.getMessage());

      // Check if circuit is open and log appropriate message
      if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
        LOGGER.warn("Telemetry circuit breaker is OPEN - telemetry events are being dropped");
      }
    }
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } finally {
      // Close the circuit breaker
      circuitBreaker.releasePermission();
    }
  }

  /**
   * Get the current state of the circuit breaker.
   *
   * @return The current circuit breaker state
   */
  public CircuitBreaker.State getCircuitBreakerState() {
    return circuitBreaker.getState();
  }

  /**
   * Get metrics about the circuit breaker.
   *
   * @return Circuit breaker metrics
   */
  public CircuitBreaker.Metrics getCircuitBreakerMetrics() {
    return circuitBreaker.getMetrics();
  }
}
