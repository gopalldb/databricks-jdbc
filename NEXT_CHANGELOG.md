# NEXT CHANGELOG

## [Unreleased]

### Added
- Added result set heartbeat / keep-alive to prevent server-side result expiry during slow consumption. When enabled via `EnableHeartbeat=1`, the driver periodically polls `GetStatementStatus` (SEA) or `GetOperationStatus` (Thrift) to keep the operation alive while the client reads results. Configurable interval via `HeartbeatIntervalSeconds` (default 60s). Heartbeat automatically stops when results are fully consumed, ResultSet is closed, or the server returns a terminal state. Disabled by default due to cost implications (heartbeats keep the warehouse running).
- Metadata operations now use SQL SHOW commands for both Thrift and SEA backends,
  ensuring consistent behavior for SQL warehouses regardless of underlying
  protocol. To revert to native Thrift metadata RPCs, set `UseQueryForMetadata=0`.
- Added `UseBoundedSeaApi` connection property (default `0`/off). When enabled, the driver uses the bounded SEA API contract for CloudFetch: sends `row_offset` on GetResultData requests and uses `next_chunk_index` for chunk discovery instead of `total_chunk_count`. Requires server support.

### Updated

### Fixed
- Fixed `StackOverflowError` / hang when closing a `ResultSet` or `Statement` with `closeOnCompletion()` enabled.
- Fixed SQL injection vulnerability in binary parameter handling.
- Fixed `setCatalog()` and `setSchema()` producing invalid SQL (e.g. `SET CATALOG ``name``) when the catalog or schema name was passed already wrapped in backticks. Backticks are now stripped before wrapping, and `getCatalog()`/`getSchema()` return the bare identifier name.
- Fixed metadata SQL generation for catalog, schema, and table identifiers containing backticks.
- Fixed SEA result truncation when direct results are disabled. Large, highly-compressible results that span multiple chunks were delivered inline via the old hybrid path and truncated to the first chunk. The SQL Execution path now uses an async (`0s`) wait timeout when direct results are disabled, so results are returned via external links and fetched in full.
- Fixed timezone-shifted TIMESTAMP values when retrieving nested complex types (STRUCT/ARRAY/MAP) with `EnableComplexDatatypeSupport=1`.
- Fixed `DatabricksDatabaseMetaData.supportsBatchUpdates()` always returning `false`, which caused batch-aware JDBC clients (e.g. Apache Hop) to skip `executeBatch()` and fall back to one INSERT per row. It now returns `true` when `EnableBatchedInserts=1`, so those clients use the optimized multi-row INSERT path.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*