# NEXT CHANGELOG

## [Unreleased]

### Added
- Added result set heartbeat / keep-alive to prevent server-side result expiry during slow consumption. When enabled via `EnableHeartbeat=1`, the driver periodically polls `GetStatementStatus` (SEA) or `GetOperationStatus` (Thrift) to keep the operation alive while the client reads results. Configurable interval via `HeartbeatIntervalSeconds` (default 60s). Heartbeat automatically stops when results are fully consumed, ResultSet is closed, or the server returns a terminal state. Disabled by default due to cost implications (heartbeats keep the warehouse running).

### Updated
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
