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

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
