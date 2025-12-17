# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- Log timestamps now explicitly display timezone.
- **[Breaking Change]** `PreparedStatement.setTimestamp(int, Timestamp, Calendar)` now properly applies Calendar timezone conversion using LocalDateTime pattern (inline with `getTimestamp`). Previously Calendar parameter was ineffective.

### Fixed
- Fix timeout exception handling to throw `SQLTimeoutException` instead of `DatabricksSQLException` when queries timeout.
- Removes dangerous global timezone modification that caused race conditions.
- Fixed `ResultSet.getLargeUpdateCount()` to return -1 instead of throwing Exception when there were no more results or result is not an update count.

---
*Note: When making changes, please add your change under the appropriate section with a brief description.*
