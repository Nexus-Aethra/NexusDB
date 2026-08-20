//! Subcommand implementations for the `nexusdb-scanner` CLI.
//!
//! One module per subcommand. Each `run` takes the [`crate::cli::Globals`]
//! and either positional args or per-command state, and writes to a
//! `std::io::Write`. Returning `Ok(0)` is the contract for "no
//! catastrophic failure". Per-page errors are surfaced inline in the output
//! stream; they never propagate as `Err`.

pub mod blame;
pub mod dbs;
pub mod header;
pub mod rescue;
pub mod verify;
pub mod vpid;
