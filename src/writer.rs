//! [`RotatingWriter`] wraps [`AlignedSbeWriter`], rolling to a new file every
//! UTC hour when writing to a directory.

use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Timelike, Utc};

use crate::util::AlignedSbeWriter;
use crate::util::dtos::SbeEncode;

/// Must match the [`AlignedSbeReader`](crate::util::AlignedSbeReader) default
/// chunk size, so output files can be read back.
pub const CHUNK_SIZE: usize = 64 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputTarget {
    /// Hourly files named `{prefix}_{YYYYMMDDTHHMMSS}.sbe` inside the directory.
    Directory { dir: PathBuf, prefix: String },
    /// A single stream to stdout; never rotates.
    Stdout,
}

pub struct RotatingWriter {
    target: OutputTarget,
    writer: AlignedSbeWriter,
    path: PathBuf,
    /// The UTC hour (truncated timestamp) the current file belongs to.
    hour: DateTime<Utc>,
}

impl RotatingWriter {
    pub fn open(target: OutputTarget, now: DateTime<Utc>) -> anyhow::Result<Self> {
        let path = path_for(&target, now)?;
        let writer = AlignedSbeWriter::create(&path, CHUNK_SIZE)
            .with_context(|| format!("failed to create output file {}", path.display()))?;
        tracing::info!(path = %path.display(), "opened output");
        Ok(Self {
            target,
            writer,
            path,
            hour: truncate_to_hour(now),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn needs_rotation(&self, now: DateTime<Utc>) -> bool {
        matches!(self.target, OutputTarget::Directory { .. }) && truncate_to_hour(now) != self.hour
    }

    /// Flushes and closes the current file, then opens the next one.
    pub fn rotate(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        self.writer.flush()?;
        let next = Self::open(self.target.clone(), now)?;
        let previous = std::mem::replace(self, next);
        tracing::info!(path = %previous.path.display(), "closed output");
        Ok(())
    }

    pub fn write<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        self.writer.write_msg(message)
    }

    /// Flushes the final chunk and closes the file.
    pub fn close(mut self) -> anyhow::Result<()> {
        self.writer.flush()?;
        tracing::info!(path = %self.path.display(), "closed output");
        Ok(())
    }
}

fn truncate_to_hour(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(ts)
}

fn path_for(target: &OutputTarget, now: DateTime<Utc>) -> anyhow::Result<PathBuf> {
    match target {
        OutputTarget::Stdout => Ok(PathBuf::from("/dev/stdout")),
        OutputTarget::Directory { dir, prefix } => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("failed to create output dir {}", dir.display()))?;
            // Seconds in the name keep a restart within the same hour from
            // truncating the earlier file.
            let name = format!("{prefix}_{}.sbe", now.format("%Y%m%dT%H%M%S"));
            Ok(dir.join(name))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain};
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn update(at: DateTime<Utc>) -> PriceLevelUpdateDomain {
        PriceLevelUpdateDomain {
            backtest_timestamp: at,
            exchange_timestamp: at,
            price_level: 500_000,
            quantity: 7_000_000,
            side: Side::Sell,
            symbol: "SYM".to_owned(),
        }
    }

    fn read_updates(path: &Path) -> Vec<PriceLevelUpdateDomain> {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
        let mut out = Vec::new();
        while let Some(msg) = reader.read_msg().unwrap() {
            let AnyMessage::PriceLevelUpdate(decoded) = msg else {
                panic!("unexpected message");
            };
            out.push(PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap());
        }
        out
    }

    #[test]
    fn rotates_on_hour_boundary_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let target = OutputTarget::Directory {
            dir: dir.path().to_owned(),
            prefix: "test".to_owned(),
        };
        let first_at = ts(1_700_000_000); // 22:13:20 UTC
        let mut writer = RotatingWriter::open(target, first_at).unwrap();
        writer.write(&update(first_at)).unwrap();
        let first_path = writer.path().to_owned();

        let same_hour = first_at + chrono::Duration::minutes(10);
        assert!(!writer.needs_rotation(same_hour));

        let next_hour = first_at + chrono::Duration::minutes(50);
        assert!(writer.needs_rotation(next_hour));
        writer.rotate(next_hour).unwrap();
        writer.write(&update(next_hour)).unwrap();
        let second_path = writer.path().to_owned();
        writer.close().unwrap();

        assert_ne!(first_path, second_path);
        assert_eq!(read_updates(&first_path), vec![update(first_at)]);
        assert_eq!(read_updates(&second_path), vec![update(next_hour)]);
    }
}
