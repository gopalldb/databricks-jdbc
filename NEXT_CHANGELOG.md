# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).
- **Breaking change:** `UseQueryForMetadata` default changed from `0` to `1`. For DBSQL warehouses, SHOW commands for Thrift metadata operations are now enabled when a server-side feature flag is active. The driver uses a two-key rollout: both the client default (`1`) and the server-side flag must be enabled. Users who explicitly set `UseQueryForMetadata=0` are unaffected — explicit settings always take priority. All-purpose clusters are unaffected (always defaults to native RPCs).

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
