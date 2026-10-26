# Changelog

## nightly-20261026-config-validation (2026-10-26)

### Fixed

- Text registers (`STR`, `UTF8`): invalid UTF-8 is no longer passed through. Each invalid byte becomes U+FFFD, matching what `encoding/json` stores on the shadow path, so a direct `timescaledb` output no longer gets a string PostgreSQL rejects.
- Release build: the `release` workflow built only `cmd/main.go`, so any second file in `cmd/` broke the release; it now builds the `./cmd` package, and the `ci` workflow builds the binary the same way.
- `timescaledb_shadow`: a NaN or infinity in a float register no longer drops the whole row. JSON cannot encode those values, so `json.Marshal` failed and every field of that device and timestamp was lost; they are now sent as `"NaN"`, `"Infinity"` and `"-Infinity"`, which PostgreSQL stores as `double precision`.

### Added

- Register validation when the config is loaded (supported datatype, valid function code, enough words for the datatype, coil limits). An invalid register is skipped with a warning instead of being dropped on every read.
- `--checkConfig` flag: validate the config and exit with status 1 if any register is invalid, without the dotenv file or network access. On the production configs of 2026-10-26 it reports the three known typos (`U64` and `I16`).
- Tests for `decodeRegisterValue` and the datatype decoders (`cmd/decode_test.go`): whole 8- and 16-bit domains, 32- and 64-bit boundaries, fuzz targets, float special values, gains, short buffers and text types.
- `FuzzDecodeText`: the text decoder against an `encoding/json` round trip.
- Tests for the shadow payload (`storage/timescale_shadow/writer_test.go`).
- README: "How values are represented" and "Testing" sections.
- CI: new `ci` workflow runs `go vet ./...` and `go test ./...` on every push and pull request; the `release` workflow now runs the tests before building binaries.
- CI: GitHub Actions updated to their current majors, all on Node 24 (`checkout` v7, `setup-go` v7, `upload-artifact` v7, `download-artifact` v8, `action-gh-release` v3). `download-artifact` v8 now fails on a hash mismatch instead of warning.
