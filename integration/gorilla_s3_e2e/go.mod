module github.com/ProjectASAP/ASAPCollector/integration/gorilla_s3_e2e

go 1.25.5

// Phase 6 Gorilla-S3 e2e integration test deps.
//
// Kept INTENTIONALLY MINIMAL — std library only — so this test is
// portable across CI environments and does not need the patched OTel
// collector replace cascade that integration/parity/ requires. The
// in-test Go decoder mirrors the GORILLA1 block format directly
// (see gorilla_decoder.go) which keeps the byte-format contract
// pinned in this PR's source tree, not via a transitive import that
// could silently drift on a dep bump.
//
// The cross-language subtest shells out to `cargo test` against the
// sibling asap-gorilla crate (../../asap-gorilla), so its
// byte-compatibility assertion uses the canonical Rust decoder
// without adding any Go-side dep.
