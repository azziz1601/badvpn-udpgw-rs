use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    None = 0,
    Error = 1,
    Warning = 2,
    Notice = 3,
    Info = 4,
    Debug = 5,
}

impl LogLevel {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "0" | "none" => Some(Self::None),
            "1" | "error" => Some(Self::Error),
            "2" | "warning" => Some(Self::Warning),
            "3" | "notice" => Some(Self::Notice),
            "4" | "info" => Some(Self::Info),
            "5" | "debug" => Some(Self::Debug),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Error => "ERROR",
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
        }
    }
}

static CURRENT_LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Notice as u8);

pub fn set_level(level: LogLevel) {
    CURRENT_LEVEL.store(level as u8, Ordering::Relaxed);
}

#[inline]
pub fn is_enabled(level: LogLevel) -> bool {
    let cur = CURRENT_LEVEL.load(Ordering::Relaxed);
    cur > 0 && (level as u8) <= cur
}

pub fn log_msg(level: LogLevel, args: std::fmt::Arguments<'_>) {
    if !is_enabled(level) {
        return;
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if level == LogLevel::Error || level == LogLevel::Warning {
        eprintln!("[{}] {}: {}", ts, level.label(), args);
    } else {
        println!("[{}] {}: {}", ts, level.label(), args);
    }
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::logger::log_msg($crate::logger::LogLevel::Error, format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::logger::log_msg($crate::logger::LogLevel::Warning, format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_notice {
    ($($arg:tt)*) => {
        $crate::logger::log_msg($crate::logger::LogLevel::Notice, format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::logger::log_msg($crate::logger::LogLevel::Info, format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::logger::is_enabled($crate::logger::LogLevel::Debug) {
            $crate::logger::log_msg($crate::logger::LogLevel::Debug, format_args!($($arg)*));
        }
    };
}
