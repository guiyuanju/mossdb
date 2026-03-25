use mossdb::common::MossError;
use mossdb::engine::Engine;
use std::fs::remove_file;
use std::thread::sleep;
use std::time::Duration;

fn clear_log_files(engine: &Engine) {
    let files = engine.list_sorted_log_files().unwrap();
    for p in files {
        remove_file(p).unwrap();
    }
}

// put
#[test]
fn test_put() {
    let e = Engine::new("./", 10, 10).unwrap();
    clear_log_files(&e);

    e.put("1", "1");
    assert_eq!("1", e.get("1").unwrap());

    clear_log_files(&e);
}

// mutliple put
#[test]
fn test_multiple_put() {
    let e = Engine::new("./", 10, 10).unwrap();
    clear_log_files(&e);

    e.put("1", "1");
    assert_eq!("1", e.get("1").unwrap());

    e.put("2", "2");
    assert_eq!("2", e.get("2").unwrap());

    clear_log_files(&e);
}

// put override
#[test]
fn test_put_override() {
    let e = Engine::new("./", 10, 10).unwrap();
    clear_log_files(&e);

    e.put("1", "1");
    assert_eq!("1", e.get("1").unwrap());

    e.put("2", "2");
    assert_eq!("2", e.get("2").unwrap());

    e.put("1", "3");
    assert_eq!("3", e.get("1").unwrap());

    clear_log_files(&e);
}

// del
#[test]
fn test_del() {
    let e = Engine::new("./", 10, 10).unwrap();
    clear_log_files(&e);

    e.put("1", "1");
    assert_eq!("1", e.get("1").unwrap());

    e.put("2", "2");
    assert_eq!("2", e.get("2").unwrap());

    e.del("1");
    assert!(e.get("1").is_err_and(|e| e == MossError::KeyNotFound));

    clear_log_files(&e);
}

// put trigger flush
#[test]
fn test_put_and_flush() {
    let e = Engine::new("./", 4, 10).unwrap();
    clear_log_files(&e);
    assert_eq!(0, e.list_sorted_log_files().unwrap().len());

    e.put("1", "111");
    assert_eq!("111", e.get("1").unwrap());
    assert_eq!(0, e.list_sorted_log_files().unwrap().len());

    e.put("2", "222");
    assert_eq!("222", e.get("2").unwrap());
    sleep(Duration::from_secs(1)); // wait for the flush thread to finish flushing, need to find a better test method
    assert_eq!(2, e.list_sorted_log_files().unwrap().len());

    clear_log_files(&e);
}

// compact
#[test]
fn test_put_del_compact() {
    let e = Engine::new("./", 1, 2).unwrap();
    clear_log_files(&e);
    assert_eq!(0, e.list_sorted_log_files().unwrap().len());

    e.put("1", "1");
    assert_eq!("1", e.get("1").unwrap());

    e.put("2", "2");
    assert_eq!("2", e.get("2").unwrap());

    e.put("1", "111");
    assert_eq!("111", e.get("1").unwrap());

    e.del("2");
    sleep(Duration::from_secs(1));
    assert_eq!(2, e.list_sorted_log_files().unwrap().len());
    assert!(e.get("2").is_err_and(|e| e == MossError::KeyNotFound));

    clear_log_files(&e);
}
