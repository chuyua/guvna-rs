# Source audit provenance

- User-supplied upload: `guvna-audit-report.zip` (October 2026).
- Original report path in archive: `guvna-audit/REPORT.md`.
- Audited Go revision: `f90d7d2`; Rust development baseline fetched as
  `a4672c25da4723ff1e165fa48be693935bc167d2`.
- Archive contents: `REPORT.md`, `RUBRIC.md`, four auditor details, two
  independent P0 verification reports, complexity/duplication measurement
  files and small Go reproduction probe.
- Archive SHA-256:
  `6b87832a141a260591f8a442524d7ea333caaf6d5ce5e2815249d0d586cf3e56`.

The lead corrected an early explanation of the SQLite time-window defect:
the sub-millisecond loss is **SQLite date-parser quantization to
milliseconds**, **not** float64 ULP. Independent A/B runs changed 26 failures
in 40 package runs to 0 in 40 after widening SQL query boundaries and exact
Go-side filtering. For Rust, compare **integer microseconds** directly.

Treat audit judgments as evidence to validate, not automatic proof that the
latest Go main still contains each issue.

The complete source archive must be copied into `docs/audit/source/` when an
attachment-transfer path is available; at present this project file records
its identifying checksum and all P0 engineering gates without pretending the
full uploaded ZIP is already checked in.
