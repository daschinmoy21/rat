//! Finite (or growing) JSONL input. The position is the byte offset just past
//! the last consumed newline, so a restart resumes at the next line, and a file
//! that has grown since the last run is read on from there.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::checkpoint::Position;
use crate::pipeline::{Fetch, Source};

pub struct FileSource {
    path: PathBuf,
    reader: BufReader<File>,
    byte_offset: u64,
    line: u64,
    /// When false, an unterminated last line is consumed at EOF (finite replay).
    /// When true it waits for its newline (file still being written).
    follow: bool,
    buf: Vec<u8>,
}

impl FileSource {
    pub fn open(path: &Path, position: &Position, follow: bool) -> Result<FileSource> {
        let (byte_offset, line) = match position {
            Position::Start => (0, 0),
            Position::File { byte_offset, line } => (*byte_offset, *line),
            other => bail!("checkpoint position {other:?} is not a file position"),
        };
        let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let len = file.metadata()?.len();
        if len < byte_offset {
            bail!(
                "{} is {len} bytes but the checkpoint is at byte {byte_offset}: the input was truncated or replaced",
                path.display()
            );
        }
        file.seek(SeekFrom::Start(byte_offset))?;
        Ok(FileSource {
            path: path.to_path_buf(),
            reader: BufReader::with_capacity(1 << 20, file),
            byte_offset,
            line,
            follow,
            buf: Vec::with_capacity(4096),
        })
    }
}

impl Source for FileSource {
    fn position(&self) -> Position {
        Position::File {
            byte_offset: self.byte_offset,
            line: self.line,
        }
    }

    fn fetch(&mut self, max: usize) -> Result<Fetch> {
        let mut records = Vec::new();
        while records.len() < max {
            self.buf.clear();
            let n = self
                .reader
                .read_until(b'\n', &mut self.buf)
                .with_context(|| format!("read {}", self.path.display()))?;
            if n == 0 {
                break;
            }
            let terminated = self.buf.last() == Some(&b'\n');
            if !terminated && self.follow {
                // Partial line: rewind and wait for the writer to finish it.
                self.reader.seek(SeekFrom::Start(self.byte_offset))?;
                break;
            }
            self.byte_offset += n as u64;
            self.line += 1;
            let mut line = self.buf.as_slice();
            while let Some((last, rest)) = line.split_last()
                && (*last == b'\n' || *last == b'\r')
            {
                line = rest;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            records.push(line.to_vec());
        }
        if records.is_empty() {
            return Ok(if self.follow { Fetch::Idle } else { Fetch::End });
        }
        Ok(Fetch::Records(records))
    }
}
