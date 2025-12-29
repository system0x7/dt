use thiserror::Error;

#[derive(Error, Debug)]
pub enum DtransformError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Regex error: {0}")]
    RegexError(#[from] regex::Error),

    #[error("Pest parse error: {0}")]
    PestError(String),

    #[error("Readline error: {0}")]
    ReadlineError(String),
}

pub type Result<T> = std::result::Result<T, DtransformError>;

impl DtransformError {
    pub fn display_friendly(&self) -> String {
        match self {
            DtransformError::ColumnNotFound(col) => {
                format!(
                    "Column '{}' not found.\nUse .schema to see all columns.",
                    col
                )
            }
            DtransformError::ParseError(msg) => {
                format!("Syntax error: {}\nSee examples with .help", msg)
            }
            DtransformError::VariableNotFound(var) => {
                format!(
                    "Variable '{}' not found.\nUse .vars to see all variables.",
                    var
                )
            }
            _ => self.to_string(),
        }
    }
}
