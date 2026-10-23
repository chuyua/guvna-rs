# Offline runtime chain management

The Rust gateway supports version-1 runtime chains in the data directory.
This is a local-only CLI, not an HTTP admin endpoint. Every management
command requires an explicit existing config file. A running daemon keeps its
current routing snapshot; restart guvna after each change to apply it.

Use the same config and data directory as the service:

    guvna chains list -config config.yaml -data /var/lib/guvna
    guvna chains validate -config config.yaml -data /var/lib/guvna

Create a chain document (the YAML file contains exactly one Chain object):

    name: work-model
    steps:
      - provider: primary
        model: upstream/model
      - provider: backup
        model: upstream/model

Then manage it using:

    guvna chains set -file work-model.yaml -config config.yaml -data /var/lib/guvna
    guvna chains delete -name work-model -config config.yaml -data /var/lib/guvna
    guvna chains recover -config config.yaml -data /var/lib/guvna

The list command returns JSON with each chain's source as "config" or
"runtime". Base config chains are read-only; trying to override or delete one
from the runtime store fails. Set creates or updates a named runtime chain.

## Safety and recovery

- Config and runtime YAML pass schema, provider reference, version and
  forbidden-parameter checks. Unknown fields, duplicate names and names that
  collide with base config are rejected by management operations.
- Live chains.yaml remains unchanged on invalid set/delete input.
  Only version 1 is writable. Non-regular/symlink runtime and lock paths
  are rejected. Runtime files are capped at 1 MiB.
- A per-data-dir advisory lock serializes independent management processes;
  a named temporary file is fsynced and atomically renamed.
  On Linux, persisted files have mode 0600, and the directory metadata
  is synced after committing.
- Before replacing a valid existing file, a backup named chains.yaml.bak
  is atomically written with the previous committed version. This is a
  single rolling backup, not an unlimited history.
- Recover validates chains.yaml.bak first (or an explicit -from PATH backup),
  preserves the current original byte-for-byte under a unique
  chains.yaml.before-recovery-*.bak file and then commits the valid source.
  Even when the current file is damaged, set/delete reject writes; only
  explicit recovery replaces damaged content.
- A missing or invalid backup never changes chains.yaml.
  Recovery preimages are kept for forensic review.

## Deployment limits

Management is offline: no live daemon reload, Unix socket or remote mutation
API is added. Use the same Linux service user and paths as the running
service. Avoid concurrent manual edits that bypass the CLI's advisory lock.
No API_KEYS or ADMIN_KEY is required for local file management, but OS
filesystem permissions for the selected data directory are required.

After a change, validate first, then restart deliberately. Corrupted runtime
chains still prevent normal daemon startup rather than being silently ignored.

The Linux release-binary CI gate exercises the CLI directly (create/update,
backup, delete, invalid YAML rejection, recovery and permissions) before
existing SSE/failover/load tests. Rust integration tests also race eight
independent CLI processes writing distinct routes to the same store.
