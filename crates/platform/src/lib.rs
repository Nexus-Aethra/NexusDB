//! NexusDB 的编译期平台能力边界。
//!
//! feature 表示可选实现能力，`cfg(target_os)` 才决定当前二进制实际运行在哪个平台。
//! 因而不允许以 `--features windows-iocp` 把 Linux 二进制伪装成 Windows 后端。

/// 当前目标平台。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetPlatform {
    Linux,
    Windows,
    MacOs,
    Other,
}

/// 由编译 target 决定的能力集合。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlatformCapabilities {
    pub target: TargetPlatform,
    pub supports_io_uring: bool,
    pub supports_iocp: bool,
    pub supports_epoll: bool,
}

#[cfg(target_os = "linux")]
pub const CURRENT: PlatformCapabilities = PlatformCapabilities {
    target: TargetPlatform::Linux,
    supports_io_uring: true,
    supports_iocp: false,
    supports_epoll: true,
};

#[cfg(target_os = "windows")]
pub const CURRENT: PlatformCapabilities = PlatformCapabilities {
    target: TargetPlatform::Windows,
    supports_io_uring: false,
    supports_iocp: true,
    supports_epoll: false,
};

#[cfg(target_os = "macos")]
pub const CURRENT: PlatformCapabilities = PlatformCapabilities {
    target: TargetPlatform::MacOs,
    supports_io_uring: false,
    supports_iocp: false,
    supports_epoll: false,
};

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub const CURRENT: PlatformCapabilities = PlatformCapabilities {
    target: TargetPlatform::Other,
    supports_io_uring: false,
    supports_iocp: false,
    supports_epoll: false,
};

/// 返回配置中的存储后端能否在当前 target 上运行。
pub fn storage_backend_supported(backend: &str) -> bool {
    match backend.to_ascii_lowercase().as_str() {
        "stdfs" => true,
        "io_uring" | "iouring" => CURRENT.supports_io_uring,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdfs_is_portable() {
        assert!(storage_backend_supported("stdfs"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_declares_its_native_capabilities() {
        assert_eq!(CURRENT.target, TargetPlatform::Linux);
        assert!(CURRENT.supports_io_uring);
        assert!(CURRENT.supports_epoll);
        assert!(!CURRENT.supports_iocp);
        assert!(storage_backend_supported("io_uring"));
    }
}
