# RCA: Leaked Socket Prevents CRaC Checkpointing (Issue #1325)

## Problem Statement

After `Connection.close()`, a `DatabricksHttpClient` with type `TELEMETRY` can remain in
`DatabricksHttpClientFactory.instances`, keeping a TCP socket open indefinitely. This prevents
CRaC (Coordinated Restore at Checkpoint) from completing because CRaC requires all sockets to be
closed before a checkpoint can be taken.

**Reporter**: @jnd77 (follow-up to #1233)
**Affected versions**: 3.x (confirmed on 3.3.1)
**Symptom**: Intermittent — depends on timing of telemetry flush tasks relative to connection close.

## Root Cause

The bug is a **cross-thread race condition** in the connection close path involving two independent
mechanisms that can re-create HTTP clients after they've been removed.

### Close Sequence (DatabricksConnection.close())

```
Line 421: session.close()
Line 422: TelemetryClientFactory.closeTelemetryClient(ctx)
Line 423: DatabricksClientConfiguratorManager.removeInstance(ctx)
Line 424: DatabricksDriverFeatureFlagsContextFactory.removeInstance(ctx)
Line 425: DatabricksHttpClientFactory.removeClient(ctx)          // removes all HTTP clients
Line 426: DatabricksThreadContextHolder.clearAllContext()
```

### Race Condition 1: TelemetryClient re-creation after close

Inside `TelemetryClientFactory.closeTelemetryClient()`, the ordering was:

1. **Remove the TelemetryClientHolder** from the map via `computeIfPresent` -> calls
   `TelemetryClient.close()` -> `flush(true).get()` (synchronous flush)
2. **Export pending TelemetryCollector events** via `collector.exportAllPendingTelemetryDetails()`

Step 2 calls `TelemetryHelper.exportTelemetryLog()` which calls
`TelemetryClientFactory.getTelemetryClient(ctx)`. Since the holder was already removed in Step 1,
`getTelemetryClient()` sees `existing == null` and **creates a brand new TelemetryClient** with a
new periodic flush scheduler. This orphaned client is never closed.

### Race Condition 2: TELEMETRY HTTP client re-creation after removeClient

`TelemetryClient.close()` calls `flush(true).get()` which submits a `TelemetryPushTask` to the
shared 10-thread executor pool. The task calls:

```
TelemetryPushClient.pushEvent()
  -> DatabricksHttpClientFactory.getClient(ctx, HttpClientType.TELEMETRY)
```

If this task executes **after** `DatabricksHttpClientFactory.removeClient(ctx)` at line 425,
`computeIfAbsent` creates a **new** `DatabricksHttpClient(TELEMETRY)` that nobody will ever close.
This leaked HTTP client holds an open TCP socket.

### Why it's intermittent

The reporter notes the issue is "random." This is because:
- The race window is between `flush().get()` completing on the main thread and the actual
  `TelemetryPushTask.run()` executing on the pool thread
- It only triggers when there are pending telemetry events at close time
- GC pauses and CPU scheduling widen or narrow the window

### Previous fix (#1235) and why it was incomplete

PR #1235 fixed the `DatabricksClientConfiguratorManager` leak (SDK connection manager not being
closed). But it did not address:
1. The telemetry client re-creation in `closeTelemetryClient()`
2. The HTTP client re-creation via `computeIfAbsent` after `removeClient()`

## Fix

The fix addresses both race conditions with a defense-in-depth approach:

### Fix 1: TelemetryClientFactory — Prevent TelemetryClient re-creation

**File**: `TelemetryClientFactory.java`

- **Added `closedConnectionUuids` set**: Tracks connection UUIDs that have been closed.
  `getTelemetryClient()` checks this set and returns `NoopTelemetryClient` for closed connections
  instead of creating a new orphaned `TelemetryClient`.

- **Reordered `closeTelemetryClient()`**: Export pending `TelemetryCollector` events **BEFORE**
  closing the `TelemetryClient`. This ensures the export uses the existing client (still in the
  holder map) rather than triggering re-creation after the holder is removed.

- The UUID is added to `closedConnectionUuids` inside the `computeIfPresent` lambda so only
  connections that actually had a telemetry client get tracked (avoids poisoning the set during
  test setup/cleanup).

### Fix 2: DatabricksHttpClientFactory — Prevent HTTP client re-creation

**File**: `DatabricksHttpClientFactory.java`

- **Added `closedConnections` set**: Tracks connection UUIDs that have been permanently closed.

- **New `closeConnection()` method**: Marks the connection as permanently closed and removes all
  HTTP clients. After this call, `getClient()` returns `null` for that connection, preventing
  `computeIfAbsent` from creating orphaned `DatabricksHttpClient` instances.

- `removeClient()` is unchanged — it still allows re-creation for non-close use cases (e.g.,
  client reset/reconnect scenarios used in tests).

### Fix 3: DatabricksConnection — Use permanent close

**File**: `DatabricksConnection.java`

- Changed `removeClient(connectionContext)` to `closeConnection(connectionContext)` to use the
  permanent close semantics that prevent HTTP client re-creation.

### Fix 4: TelemetryPushClient — Null guard

**File**: `TelemetryPushClient.java`

- `pushEvent()` now handles `null` return from `getClient()` gracefully (logs and returns early)
  instead of throwing NPE. This is the safety net for delayed push tasks that fire after the
  connection is closed.

## Reproduction and Verification Plan

### Automated Tests (TelemetryHttpClientLeakTest.java)

Three unit tests reproduce the two race conditions:

#### Test 1: `testGetTelemetryClientAfterCloseReCreatesClient`

Reproduces Race Condition 1.

**Steps**:
1. Create a mock connection context with telemetry enabled
2. Call `getTelemetryClient(ctx)` — creates a `TelemetryClient` in the holder map
3. Call `closeTelemetryClient(ctx)` — removes the holder
4. Call `getTelemetryClient(ctx)` again (simulates what `exportAllPendingTelemetryDetails` does)
5. **Assert**: The returned client should be `NoopTelemetryClient`, not a new `TelemetryClient`

**Before fix**: Returns a new `TelemetryClient` (FAIL — orphaned client created)
**After fix**: Returns `NoopTelemetryClient` (PASS — no leak)

#### Test 2: `testGetClientReturnsNullAfterCloseConnection`

Reproduces Race Condition 2.

**Steps**:
1. Create a mock connection context
2. Call `DatabricksHttpClientFactory.closeConnection(ctx)` (simulates `DatabricksConnection.close()`)
3. Call `getClient(ctx, HttpClientType.TELEMETRY)` (simulates delayed `TelemetryPushTask`)
4. **Assert**: Returns `null` (not a new `DatabricksHttpClient`)

**Before fix**: Creates a new `DatabricksHttpClient` via `computeIfAbsent` (FAIL — leaked socket)
**After fix**: Returns `null` (PASS — no leak)

#### Test 3: `testCloseTelemetryClientWithPendingCollectorEventsReCreatesClient`

End-to-end test with pending telemetry collector events.

**Steps**:
1. Create a telemetry client and record pending latency events in `TelemetryCollector`
2. Mock `exportTelemetryLog` to call `getTelemetryClient(ctx)` (simulating the real export path)
3. Call `closeTelemetryClient(ctx)` which triggers the collector export
4. **Assert**: No new `TelemetryClient` holders exist after close

### Running the tests

```bash
# Run just the leak reproduction tests
mvn test -pl jdbc-core -Dtest=TelemetryHttpClientLeakTest -Djacoco.skip=true

# Run all telemetry tests (existing + new)
mvn test -pl jdbc-core -Dtest="TelemetryClientFactoryTest,TelemetryClientTest,TelemetryPushClientTest,TelemetryCollectorManagerTest,TelemetryCollectorTest,TelemetryHelperTest,TelemetryHttpClientLeakTest" -Djacoco.skip=true

# Run full unit test suite
mvn test -pl jdbc-core -Djacoco.skip=true -Dgroups='!Jvm17PlusAndArrowToNioReflectionDisabled'
```

### Manual verification (with CRaC-enabled JDK)

Use the reporter's reproducer from issue #1233 to verify 0 sockets remain after close:

1. Build the driver: `mvn clean install -DskipTests`
2. Set environment variables:
   ```bash
   export DATABRICKS_AUTH_TOKEN=<token>
   export DATABRICKS_CONNECTION_STRING="jdbc:databricks://<host>:443/default;transportMode=http;ssl=1;httpPath=<path>;AuthMech=3;UID=token"
   ```
3. Run the socket leak reproducer (from issue #1233) which:
   - Opens a connection, executes `SELECT 1`, closes the connection
   - Calls `GlobalAsyncHttpClient.releaseClient()`
   - Checks for remaining TCP sockets via `ss -tnp state established dst :443`
4. **Expected**: 0 sockets after close
5. Run the CRaC checkpoint reproducer:
   - Same steps but calls `Core.checkpointRestore()` after close
   - **Expected**: Checkpoint succeeds without `CheckpointOpenSocketException`

### Regression testing

The fix does not change any public API or behavior for active connections. It only prevents
resource re-creation after close. The full unit test suite (3085 tests) passes with 0 failures.

## Files Changed

| File | Change |
|------|--------|
| `TelemetryClientFactory.java` | Added `closedConnectionUuids` guard, reordered close sequence |
| `DatabricksHttpClientFactory.java` | Added `closedConnections` guard, new `closeConnection()` method |
| `DatabricksConnection.java` | Use `closeConnection()` instead of `removeClient()` |
| `TelemetryPushClient.java` | Null guard for `getClient()` return value |
| `TelemetryHttpClientLeakTest.java` | 3 reproduction tests |
