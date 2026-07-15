# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Fixed `IdleConnectionEvictor` thread leak when `Connection.close()` is called after a short-lived auth token has expired. Previously, the 401 from `deleteSession()` propagated out of `close()` before the HTTP client was released, leaving a background evictor thread running permanently per closed connection. Driver-side resource cleanup now runs unconditionally in a `finally` block regardless of server-side close errors.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
