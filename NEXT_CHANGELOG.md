# NEXT CHANGELOG

## [Unreleased]

### BREAKING CHANGES in 3.4.1

1. **`getTables()`: Percent sign (`%`) in catalog argument is now treated as a literal character, not a wildcard.** Previously returned all tables; now returns zero rows unless a catalog named "%" exists. JDBC spec: catalog is an exact-match parameter, not a pattern. Migration: Pass `null` to search all catalogs.

2. **`getColumnTypeName()`: DECIMAL columns now return `"DECIMAL"` without precision/scale** (e.g., `"DECIMAL"` not `"DECIMAL(10,2)"`). Use `getPrecision()` and `getScale()` for numeric constraints. JDBC spec: `getColumnTypeName()` returns the base type name only.

3. **For DBSQL warehouses, metadata operations are now powered by SHOW SQL commands.** SQL Exec API mode already was powered by SHOW commands, now the same is true for Thrift server mode as well. To revert to native Thrift metadata RPCs, set `UseQueryForMetadata` to `0`.

### Added

### Updated
- `EnableGeoSpatialSupport` no longer requires `EnableComplexDatatypeSupport=1`. Geospatial types (GEOMETRY, GEOGRAPHY) can now be enabled independently of complex type support (ARRAY, MAP, STRUCT).

### Fixed

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
