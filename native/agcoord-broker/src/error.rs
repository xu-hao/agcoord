use std::fmt;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug)]
pub struct AppError {
    pub code: &'static str,
    pub message: String,
    pub exit_status: u8,
}

impl AppError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            exit_status: 1,
        }
    }

    pub fn usage(message: impl Into<String>) -> Self {
        Self {
            code: "broker-command-invalid",
            message: message.into(),
            exit_status: 2,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AppError {}
