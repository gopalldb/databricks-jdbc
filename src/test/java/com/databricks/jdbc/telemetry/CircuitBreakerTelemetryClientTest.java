package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CircuitBreakerTelemetryClientTest {

  @Mock private ITelemetryClient mockDelegate;

  private CircuitBreakerTelemetryClient circuitBreakerClient;
  private TelemetryFrontendLog testEvent;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    testEvent = new TelemetryFrontendLog();
    circuitBreakerClient = new CircuitBreakerTelemetryClient(mockDelegate);
  }

  @Test
  void testSuccessfulExportEvent() {
    // Given: delegate works normally
    doNothing().when(mockDelegate).exportEvent(any(TelemetryFrontendLog.class));

    // When: exporting an event
    circuitBreakerClient.exportEvent(testEvent);

    // Then: delegate should be called
    verify(mockDelegate, times(1)).exportEvent(testEvent);
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreakerClient.getCircuitBreakerState());
  }

  @Test
  void testCircuitBreakerOpensOnFailures() {
    // Given: delegate throws exceptions
    doThrow(new ConnectException("Connection failed"))
        .when(mockDelegate)
        .exportEvent(any(TelemetryFrontendLog.class));

    // When: making multiple calls that fail
    for (int i = 0; i < 15; i++) {
      circuitBreakerClient.exportEvent(testEvent);
    }

    // Then: circuit breaker should open after enough failures
    assertEquals(CircuitBreaker.State.OPEN, circuitBreakerClient.getCircuitBreakerState());

    // And: subsequent calls should not reach the delegate
    circuitBreakerClient.exportEvent(testEvent);
    verify(mockDelegate, atMost(15)).exportEvent(any(TelemetryFrontendLog.class));
  }

  @Test
  void testCircuitBreakerRecovery() {
    // Given: delegate initially fails then succeeds
    doThrow(new ConnectException("Connection failed"))
        .doNothing()
        .when(mockDelegate)
        .exportEvent(any(TelemetryFrontendLog.class));

    // When: making calls that initially fail
    for (int i = 0; i < 5; i++) {
      circuitBreakerClient.exportEvent(testEvent);
    }

    // Then: circuit should still be closed (not enough failures)
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreakerClient.getCircuitBreakerState());
  }

  @Test
  void testCloseDelegatesToDelegate() {
    // When: closing the circuit breaker client
    circuitBreakerClient.close();

    // Then: delegate should be closed
    verify(mockDelegate, times(1)).close();
  }

  @Test
  void testMetricsAreAvailable() {
    // When: getting metrics
    CircuitBreaker.Metrics metrics = circuitBreakerClient.getCircuitBreakerMetrics();

    // Then: metrics should not be null
    assertNotNull(metrics);
    assertEquals(0, metrics.getNumberOfFailedCalls());
    assertEquals(0, metrics.getNumberOfSuccessfulCalls());
  }
}
