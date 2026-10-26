# Changelog

## Unreleased

### Fixed

- `timescaledb_shadow`: a NaN or infinity in a float register no longer drops the whole row. JSON cannot encode those values, so `json.Marshal` failed and every field of that device and timestamp was lost; they are now sent as `"NaN"`, `"Infinity"` and `"-Infinity"`, which PostgreSQL stores as `double precision`.

### Added

- Tests for `decodeRegisterValue` and the datatype decoders (`cmd/decode_test.go`): whole 8- and 16-bit domains, 32- and 64-bit boundaries, fuzz targets, float special values, gains, short buffers and text types.
- Tests for the shadow payload (`storage/timescale_shadow/writer_test.go`).
- README: "How values are represented" and "Testing" sections.
- CI: new `ci` workflow runs `go vet ./...` and `go test ./...` on every push and pull request; the `release` workflow now runs the tests before building binaries.
