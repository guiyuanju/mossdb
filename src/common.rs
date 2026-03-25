use crate::layout::LOG_FILE_EXT;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

pub fn next_log_file_name(dir: &str) -> String {
    let name = Uuid::now_v7().to_string();
    let mut path = PathBuf::new();
    path.push(dir);
    let filename = format!("{}.{}", name, LOG_FILE_EXT);
    path.push(filename);
    path.to_string_lossy().to_string()
}

#[derive(Error, Debug, PartialEq)]
pub enum MossError {
    #[error("key not found")]
    KeyNotFound,
}
