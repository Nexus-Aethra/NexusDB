//! 跨平台的带偏移文件 I/O 小接口。
//!
//! Linux 使用 `FileExt::{read_at,write_at}`；Windows 使用
//! `FileExt::{seek_read,seek_write}`。上层页、WAL 与恢复逻辑不再感知平台扩展 trait。

use std::fs::File;
use std::io;
#[cfg(not(target_os = "linux"))]
use std::io::ErrorKind;

pub trait FileAt {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;
}

#[cfg(target_os = "linux")]
impl FileAt for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(self, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(self, buf, offset)
    }
}

#[cfg(target_os = "windows")]
impl FileAt for File {
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = std::os::windows::fs::FileExt::seek_read(self, buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "short positioned read",
                ));
            }
            offset += n as u64;
            buf = &mut buf[n..];
        }
        Ok(())
    }

    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            let n = std::os::windows::fs::FileExt::seek_write(self, buf, offset)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "short positioned write",
                ));
            }
            offset += n as u64;
            buf = &buf[n..];
        }
        Ok(())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(self, buf, offset)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
impl FileAt for File {
    fn read_exact_at(&self, _buf: &mut [u8], _offset: u64) -> io::Result<()> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "positioned file IO unsupported on this target",
        ))
    }

    fn write_all_at(&self, _buf: &[u8], _offset: u64) -> io::Result<()> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "positioned file IO unsupported on this target",
        ))
    }

    fn write_at(&self, _buf: &[u8], _offset: u64) -> io::Result<usize> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "positioned file IO unsupported on this target",
        ))
    }
}
