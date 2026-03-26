package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Reproduction test for GitHub issue #1325: Leaked Socket prevents CRaC checkpointing.
 *
 * <p>Root cause: In TelemetryClientFactory.closeTelemetryClient(), the TelemetryCollector's pending
 * events are exported AFTER the TelemetryClient has been closed and removed from the holder map.
 * The export path calls getTelemetryClient(ctx) which re-creates a new TelemetryClient. That new
 * client's eventual flush calls DatabricksHttpClientFactory.getClient(ctx, TELEMETRY) — and since
 * removeClient(ctx) already ran, this creates an orphaned HTTP client that leaks a TCP socket.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TelemetryHttpClientLeakTest {

  @BeforeEach
  public void setUp() {
    TelemetryClientFactory.getInstance().reset();
    TelemetryCollectorManager.getInstance().clear();
  }

  @AfterEach
  public void tearDown() {
    TelemetryClientFactory.getInstance().reset();
    TelemetryCollectorManager.getInstance().clear();
  }

  /**
   * Proves the fundamental re-creation bug: calling getTelemetryClient() after
   * closeTelemetryClient() creates a new orphaned TelemetryClient.
   *
   * <p>This is exactly what happens inside closeTelemetryClient() at line 172 when
   * collector.exportAllPendingTelemetryDetails() → TelemetryHelper.exportTelemetryLog() →
   * TelemetryClientFactory.getTelemetryClient(ctx) is called after the holder was removed at line
   * 149.
   */
  @Test
  void testGetTelemetryClientAfterCloseReCreatesClient() throws Exception {
    String host = "leak-test-host.databricks.net";
    String uuid = "leak-test-uuid-1";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      IDatabricksConnectionContext ctx = createMockContext(uuid, host);

      // Open: create telemetry client
      ITelemetryClient client = TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
      assertInstanceOf(TelemetryClient.class, client);
      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Close: remove telemetry client
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Bug: getTelemetryClient after close re-creates a new TelemetryClient
      ITelemetryClient recreated = TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      // After closeTelemetryClient(), getTelemetryClient() should NOT create a new
      // real TelemetryClient. It should return NoopTelemetryClient to prevent leaks.
      assertInstanceOf(
          NoopTelemetryClient.class,
          recreated,
          "LEAK BUG (issue #1325): getTelemetryClient() after closeTelemetryClient() "
              + "created a new TelemetryClient instead of returning NoopTelemetryClient. "
              + "This orphaned client will never be closed, and its periodic flush creates "
              + "TELEMETRY HTTP clients that leak TCP sockets.");

      assertEquals(
          0,
          TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size(),
          "LEAK BUG (issue #1325): A new TelemetryClient holder was created after close.");
    }
  }

  /**
   * Verifies that DatabricksHttpClientFactory.getClient() returns null for closed connections,
   * preventing TelemetryPushTask from creating orphaned HTTP clients after closeConnection().
   *
   * <p>Before the fix, getClient(ctx, TELEMETRY) after closeConnection(ctx) would create a new
   * DatabricksHttpClient via computeIfAbsent that was never closed, leaking a TCP socket.
   */
  @Test
  void testGetClientReturnsNullAfterCloseConnection() throws Exception {
    String uuid = "leak-test-uuid-2";
    IDatabricksConnectionContext ctx = createMockContext(uuid, "leak-test-host.databricks.net");

    // closeConnection marks the connection as permanently closed
    DatabricksHttpClientFactory.getInstance().closeConnection(ctx);

    // After closeConnection, getClient should return null (not create a new HTTP client)
    IDatabricksHttpClient client =
        DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.TELEMETRY);
    assertNull(
        client,
        "getClient() should return null for a closed connection to prevent creating "
            + "orphaned HTTP clients that leak TCP sockets (issue #1325).");

    // Also verify for other client types
    assertNull(
        DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.COMMON),
        "getClient(COMMON) should return null for closed connection");
    assertNull(
        DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.VOLUME),
        "getClient(VOLUME) should return null for closed connection");
  }

  /**
   * End-to-end test: proves the ordering bug in closeTelemetryClient causes getTelemetryClient
   * re-creation when the TelemetryCollector has pending events.
   *
   * <p>The mock of exportTelemetryLog simulates the real behavior: calling getTelemetryClient(ctx)
   * from within the export path, which happens after the holder was already removed.
   */
  @Test
  void testCloseTelemetryClientWithPendingCollectorEventsReCreatesClient() throws Exception {
    String host = "leak-test-host.databricks.net";
    String uuid = "leak-test-uuid-3";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      IDatabricksConnectionContext ctx = createMockContext(uuid, host);

      // Create telemetry client
      TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      // Record pending telemetry events in the collector
      TelemetryCollector collector =
          TelemetryCollectorManager.getInstance().getOrCreateCollector(ctx);
      collector.recordChunkDownloadLatency("stmt-1", 0, 100L);

      // Mock exportTelemetryLog to simulate the exact call chain that triggers the leak:
      // exportAllPendingTelemetryDetails → exportTelemetryLog → getTelemetryClient(ctx)
      AtomicInteger reCreationCount = new AtomicInteger(0);
      mockedStatic
          .when(() -> TelemetryHelper.exportTelemetryLog(any(), any()))
          .thenAnswer(
              invocation -> {
                int holdersBefore =
                    TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
                TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
                int holdersAfter =
                    TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
                if (holdersAfter > holdersBefore) {
                  reCreationCount.incrementAndGet();
                }
                return null;
              });

      // Call closeTelemetryClient — this triggers the bug if pending events exist
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);

      int holdersAfterClose =
          TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();

      // If the holder was re-created, the bug exists
      if (holdersAfterClose > 0 || reCreationCount.get() > 0) {
        fail(
            "BUG REPRODUCED (issue #1325): closeTelemetryClient() with pending collector "
                + "events caused TelemetryClient re-creation. "
                + "Holders after close: "
                + holdersAfterClose
                + ", re-creation events: "
                + reCreationCount.get()
                + ". The re-created client's flush will create orphaned TELEMETRY HTTP "
                + "clients that leak TCP sockets.");
      }
    }
  }

  // --- Helper methods ---

  private IDatabricksConnectionContext createMockContext(String uuid, String host) {
    IDatabricksConnectionContext ctx = mock(IDatabricksConnectionContext.class);
    when(ctx.getConnectionUuid()).thenReturn(uuid);
    when(ctx.getHost()).thenReturn(host);
    when(ctx.getHostForOAuth()).thenReturn(host);
    when(ctx.isTelemetryEnabled()).thenReturn(true);
    when(ctx.getTelemetryBatchSize()).thenReturn(10);
    when(ctx.getTelemetryFlushIntervalInMilliseconds()).thenReturn(5000);
    when(ctx.getTelemetrySocketTimeout()).thenReturn(5);
    when(ctx.isTelemetryCircuitBreakerEnabled()).thenReturn(false);
    try {
      when(ctx.getHostUrl()).thenReturn("https://" + host);
    } catch (Exception e) {
      // getHostUrl declares checked exceptions
    }
    return ctx;
  }

  private void setupTelemetryHelperMock(MockedStatic<TelemetryHelper> mockedStatic) {
    mockedStatic.when(() -> TelemetryHelper.keyOf(any())).thenCallRealMethod();
    mockedStatic.when(() -> TelemetryHelper.getDatabricksConfigSafely(any())).thenReturn(null);
    mockedStatic
        .when(() -> TelemetryHelper.removeConnectionParameters(anyString()))
        .thenAnswer(invocation -> null);
    mockedStatic
        .when(() -> TelemetryHelper.isTelemetryAllowedForConnection(any()))
        .thenReturn(true);
    mockedStatic
        .when(() -> TelemetryHelper.exportTelemetryLog(any(), any()))
        .thenAnswer(invocation -> null);
  }
}
