package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ResultHeartbeatManagerTest {

  private ResultHeartbeatManager manager;

  @AfterEach
  void tearDown() {
    if (manager != null && !manager.isShutdown()) {
      manager.shutdown();
    }
  }

  @Test
  void testStartAndStopHeartbeat() throws Exception {
    manager = new ResultHeartbeatManager(1);
    StatementId id = new StatementId("test-stmt-1");
    AtomicInteger counter = new AtomicInteger(0);

    manager.startHeartbeat(id, counter::incrementAndGet);
    assertEquals(1, manager.getActiveHeartbeatCount());

    // Wait for at least one execution
    Thread.sleep(1500);
    assertTrue(counter.get() >= 1, "Heartbeat should have executed at least once");

    manager.stopHeartbeat(id);
    assertEquals(0, manager.getActiveHeartbeatCount());

    int countAfterStop = counter.get();
    Thread.sleep(1500);
    assertEquals(countAfterStop, counter.get(), "No more heartbeats after stop");
  }

  @Test
  void testStopIsIdempotent() {
    manager = new ResultHeartbeatManager(60);
    StatementId id = new StatementId("test-stmt-2");

    manager.startHeartbeat(id, () -> {});
    manager.stopHeartbeat(id);
    assertDoesNotThrow(() -> manager.stopHeartbeat(id));
    assertDoesNotThrow(() -> manager.stopHeartbeat(new StatementId("nonexistent")));
    assertDoesNotThrow(() -> manager.stopHeartbeat(null));
  }

  @Test
  void testShutdownCancelsAll() {
    manager = new ResultHeartbeatManager(60);
    manager.startHeartbeat(new StatementId("a"), () -> {});
    manager.startHeartbeat(new StatementId("b"), () -> {});
    manager.startHeartbeat(new StatementId("c"), () -> {});
    assertEquals(3, manager.getActiveHeartbeatCount());

    manager.shutdown();
    assertEquals(0, manager.getActiveHeartbeatCount());
    assertTrue(manager.isShutdown());
  }

  @Test
  void testStartAfterShutdownIsNoOp() {
    manager = new ResultHeartbeatManager(60);
    manager.shutdown();

    manager.startHeartbeat(new StatementId("late"), () -> {});
    assertEquals(0, manager.getActiveHeartbeatCount());
  }

  @Test
  void testReExecutionReplacesHeartbeat() throws Exception {
    manager = new ResultHeartbeatManager(1);
    StatementId id = new StatementId("reuse");
    AtomicInteger firstCounter = new AtomicInteger(0);
    AtomicInteger secondCounter = new AtomicInteger(0);

    manager.startHeartbeat(id, firstCounter::incrementAndGet);
    Thread.sleep(1500);
    assertTrue(firstCounter.get() >= 1);

    // Re-start with new task (simulates re-execution)
    manager.startHeartbeat(id, secondCounter::incrementAndGet);
    assertEquals(1, manager.getActiveHeartbeatCount());

    int firstCountAtReplace = firstCounter.get();
    Thread.sleep(1500);
    assertTrue(secondCounter.get() >= 1, "New heartbeat should execute");
    assertEquals(firstCountAtReplace, firstCounter.get(), "Old heartbeat should no longer execute");
  }

  @Test
  void testHeartbeatExecutesAtInterval() throws Exception {
    manager = new ResultHeartbeatManager(1);
    CountDownLatch latch = new CountDownLatch(3);

    manager.startHeartbeat(new StatementId("interval"), () -> latch.countDown());

    boolean completed = latch.await(5, TimeUnit.SECONDS);
    assertTrue(completed, "Heartbeat should have executed 3 times within 5 seconds");
  }

  @Test
  void testNullStatementIdHandled() {
    manager = new ResultHeartbeatManager(60);
    assertDoesNotThrow(() -> manager.startHeartbeat(null, () -> {}));
    assertDoesNotThrow(() -> manager.stopHeartbeat(null));
    assertEquals(0, manager.getActiveHeartbeatCount());
  }
}
