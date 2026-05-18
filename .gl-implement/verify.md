# MaxRows Verification Plan

## Objective
Verify that DatabricksResultSet.next() enforces client-side maxRows truncation
per JDBC spec: Statement.setMaxRows() limits the number of rows returned from
any generated ResultSet. Excess rows are silently dropped.

## Approach
Use the @VisibleForTesting constructor of DatabricksResultSet to create instances
with a mocked IExecutionResult and a mocked IDatabricksStatementInternal that
returns a specific maxRows value via getMaxRows().

## Scenarios

1. **maxRows=5, server returns 100 rows**: next() should return true exactly 5
   times, then return false on the 6th call, even though the mock still has rows.

2. **maxRows=0 (no limit)**: All rows from the underlying result should be returned.
   next() should return true for every row the mock provides.

3. **maxRows=1**: Only 1 row returned. next() returns true once, then false.

4. **Empty result set with maxRows set**: If the underlying result has 0 rows,
   next() returns false immediately regardless of maxRows setting.

5. **getUpdateCount bypass**: Internal DML row counting via getUpdateCount()
   should NOT be capped by maxRows. This is tested by verifying that the
   countingUpdateRows flag bypasses the maxRows check.

## Mocking Strategy
- Mock IExecutionResult: control next() to return true N times then false
- Mock IDatabricksStatementInternal: return desired maxRows value
- Use real StatementStatus with SUCCEEDED state
- Use real StatementId with a dummy value
- DatabricksResultSetMetaData can be mocked (not exercised by next())
