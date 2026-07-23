# NEXT CHANGELOG

## [Unreleased]

### Added
- Added `UseBoundedSeaApi` connection property (default `0`/off). When enabled, the driver uses the bounded SEA API contract for CloudFetch: sends `row_offset` on GetResultData requests and uses `next_chunk_index` for chunk discovery instead of `total_chunk_count`. Requires server support.

### Updated

### Fixed
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

- Throw `DatabricksSQLException` instead of an unchecked `ClassCastException` when a complex-type getter (`getArray`, `getStruct`, `getMap`) is called on a column of a different complex type.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
