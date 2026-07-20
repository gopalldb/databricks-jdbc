# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `IdleConnectionEvictor` thread leak in long-running applications. Driver-side resources (HTTP client, background threads) are now always released when `Connection.close()` is called, even if statement cleanup or server-side session termination fails.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
