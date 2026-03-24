use uuid::Uuid;

use crate::layout::LOG_FILE_EXT;

pub fn next_log_file_name() -> String {
    let name = Uuid::now_v7().to_string();
    format!("{}.{}", name, LOG_FILE_EXT)
}
