# On the Windows AgentDock host, rustup and Cargo live below a user directory
# containing non-ASCII characters. The GNU Windows linker cannot always
# interpret those paths; ASCII junctions avoid changing the installed toolchain.
# No global environment variables are modified.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$publicDir = Join-Path ([System.IO.Path]::GetPathRoot($HOME)) 'Users\Public'
$cache = Join-Path $publicDir 'guvna-cargo-home'
$rustup = Join-Path $publicDir 'guvna-rustup-home'
$target = Join-Path $publicDir 'guvna-rs-target'
foreach ($junction in @(
    @{ Link = $cache; Target = (Join-Path $HOME '.cargo') },
    @{ Link = $rustup; Target = (Join-Path $HOME '.rustup') }
)) {
    if (!(Test-Path $junction.Link)) {
        New-Item -ItemType Junction -Path $junction.Link -Target $junction.Target | Out-Null
    }
}
$toolchain = Join-Path $rustup 'toolchains\stable-x86_64-pc-windows-gnu'
if (!(Test-Path $toolchain)) { throw "Rust GNU toolchain not found: $toolchain" }
$env:CARGO_HOME = $cache
$env:CARGO_TARGET_DIR = $target
$env:RUSTFLAGS = "--sysroot=$toolchain"
& cargo fmt --manifest-path (Join-Path $root 'Cargo.toml') --all -- --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& cargo clippy --manifest-path (Join-Path $root 'Cargo.toml') --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& cargo test --manifest-path (Join-Path $root 'Cargo.toml') --locked
exit $LASTEXITCODE
