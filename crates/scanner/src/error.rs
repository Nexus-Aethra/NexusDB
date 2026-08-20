//! Error type for the offline scanner.
//!
//! The scanner is read-only and tolerant by default — most fallible paths
//! translate a failed read into a structured `Bad*` row rather than returning
//! an `Err`. Errors here are reserved for cases where continuing past the
//! failure would be misleading (the directory itself is missing, the user
//! asked for an unambiguous lookup that cannot be served, etc.).

use std::io;
use thiserror::Error;

// `InvalidFlags` is reserved for the strict-vs-tolerant validator and
// `walk` flag checks; arrive with PR2+.
#[allow(dead_code)]

/// All errors that can be returned from scanner internals.
#[derive(Debug, Error)]
pub enum ScannerError {
    /// The directory does not exist or is not a directory.
    #[error("data directory not accessible: {path} ({source})")]
    DirNotAccessible {
        path: std::path::PathBuf,
        #[source]
        source: io::Error,
    },

    /// A user-facing flag combination is not legal (e.g. `--strict` together
    /// with a command that always tolerates).
    #[error("invalid flag combination: {0}")]
    InvalidFlags(String),

    /// A required vpid could not be located on disk and we cannot proceed.
    #[error("vpid {vpid} cannot be located on disk: {reason}")]
    VpidUnlocatable { vpid: u64, reason: String },

    /// The on-disk directory is empty (no .block files, no page.mate, no
    /// pid.state). Almost certainly the user pointed us at the wrong path.
    #[error("data directory {path} contains no recognisable NexusDB artifacts")]
    EmptyDirectory { path: std::path::PathBuf },

    /// Disk read failure that is recoverable in tolerant mode but a hard
    /// error in strict mode. Surfaced so callers can decide.
    #[error("I/O error reading {path} at offset {offset}: {source}")]
    ReadFailed {
        path: std::path::PathBuf,
        offset: u64,
        #[source]
        source: io::Error,
    },
}

impl From<io::Error> for ScannerError {
    fn from(source: io::Error) -> Self {
        ScannerError::ReadFailed {
            path: std::path::PathBuf::from("<stdout/stderr>"),
            offset: 0,
            source,
        }
    }
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, ScannerError>;
