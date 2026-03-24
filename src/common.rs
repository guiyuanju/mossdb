use std::path::PathBuf;

use uuid::Uuid;

use crate::layout::LOG_FILE_EXT;

pub fn next_log_file_name(dir: &str) -> String {
    let name = Uuid::now_v7().to_string();
    let mut path = PathBuf::new();
    path.push(dir);
    let filename = format!("{}.{}", name, LOG_FILE_EXT);
    path.push(filename);
    path.to_string_lossy().to_string()
}
