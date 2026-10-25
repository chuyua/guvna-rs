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

## Startup and file safety parity

The gateway's daemon startup and CLI use one bounded reader and validator
for chains.yaml. Duplicate names, shadowing a base route, unsupported
versions, empty steps or malformed YAML stop startup with a clear error.
No route silently disappears, and unsafe content is not auto-overwritten.

Linux uses O_NOFOLLOW on the runtime and lock files before accessing them.
The already-open file descriptor is inspected, closing the symlink-swap
gap between pathname inspection and read. Reads are limited to 1 MiB even
when another process extends the file mid-read. File backup/recovery
semantics remain unchanged, including explicit operator recovery.

## Deployment limits

Management stays local: when a running daemon exposes its permission-restricted
Unix control socket, supported chain mutations apply online; otherwise they
are written offline and require a deliberate restart. No remote management
TCP endpoint or Web API is added. Use the same Linux service user and paths
as the running service. Avoid concurrent manual edits that bypass the CLI's
advisory lock.
No API_KEYS or ADMIN_KEY is required for local file management, but OS
filesystem permissions for the selected data directory are required.
For the online Unix control socket, the existing data directory must belong
to the running service UID and must not be group- or world-writable (for
example, 0700 or 0755 is accepted; 0775 and 0777 are rejected). The daemon
does not silently change existing directory permissions. Local CLI clients
also refuse to connect through an unsafe directory to prevent socket
replacement or impersonation by another local user.

After a change, validate first, then restart deliberately. Corrupted runtime
chains still prevent normal daemon startup rather than being silently ignored.

The Linux release-binary CI gate exercises the CLI directly (create/update,
backup, delete, invalid YAML rejection, recovery and permissions) before
existing SSE/failover/load tests. Rust integration tests also race eight
independent CLI processes writing distinct routes to the same store.

## Issue #129: same-UID online runtime chain updates

On Linux, while guvna is running with its permission-restricted local
control.sock, guvna chains set --file and chains delete --name now send the
validated operation through that same-UID Unix socket. The daemon uses the
existing serialized filesystem lock and atomic chain_store write, reloads
the complete validated static+runtime route table and swaps it in one
pointer operation. The named route snapshot, authenticated /v1/models list
and /v1/models/{model} detail are derived from the same immutable generation:
concurrent readers cannot see a half-published route and stale model listing
inside a single snapshot. In-flight requests keep their old route snapshot.
Request and SSE-terminal telemetry also retain the admitted route generation
for provider/model identity: a newly added named model is recorded with its
exact model name (not the generic passthrough label), and deleting a chain
cannot relabel a stream that was already in flight. Unknown direct
passthrough model names remain excluded from verified model-health evidence.
CLI operations without a running control socket still update offline files,
with a restart warning, preserving existing behavior. No TCP control route
or Web backend exists. Chain names from static config remain read-only.
With `auto.discover_from_chains: true`, online chain additions and deletions
also rebuild the bounded Auto candidate set in the same published generation.
Before changing `chains.yaml` (offline or online), set/delete/recover verify
that enabled Auto would still have at least one eligible discovered or explicit
candidate on the next boot. Removing or replacing the **last** discoverable
candidate is rejected *without changing the live file or its backup*. This
prevents a successful runtime edit from silently making the daemon unable to
restart. Explicit configured Auto candidates allow all runtime chains to be
removed safely.
Explicit Auto candidates remain unchanged. The Auto route cache uses that
generation to discard obsolete entries; retained provider/model health scores
follow their exact identities even when candidate indexes shift. In-flight
requests and SSE streams keep their admission-time health snapshot, while
newly discovered candidates begin with neutral in-memory health. The optional
SQLite telemetry ranker refuses stale candidate generations until its next
background refresh; Decider's cache key includes the eligible candidate set.
No restart, remote management endpoint, or second dispatch loop is required.
