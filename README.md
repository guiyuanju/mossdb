# MossDB

<img width="100" alt="mossdb" src="./resources/logo.png" />

## Usage

```rust
// initialize the Engine
// with current path as the log storage, memtable max 64 MB before flush, max 10 sstable log files
let e = Engine::new("./", 64 * 1024 * 1024, 10).unwrap();

// put a key value
e.put("1", "1");

// get a key
let res = e.get("1").unwrap();
assert_eq!("1", res);

// delete a key
e.del("1");

// get a non-exist key returns an Err
let res = e.get("1");
assert!(res.is_err_and(|e| e == MossError::KeyNotFound));
```

## Architecture

![](./resources/arch.png)

**engine**: interface, providing put, get, del method, owns a memtable and current version

**memtable**: read and write

**version**: immutable snapshot of a consistent system status, owns immutable memtables and sstables

**sstable**: representation of disk log files, has sparse index and cached reader

**sparse index**: key -> block start offset

**cached reader**: cache recent accessed block

**flush thread**: flush immutable memtable to sstable files, generate new version

**ompact thread**: compact sstable files, generate new version

**sstable files**: block based, format: sparse index start, data block start, sparse index blocks, data blocks

**metadata file**: persist the order of sstable files

## Detail

### Version

![](./resources/version.png)

Version contains a consistent snapshot of system status, including a immutable memtabe queue and a sstable queue.

Flush thread and compact thread read current version and generate a new version from it, then with a optimistic lock (compare and set) to try installing the newest version.

Since the version installation is relatively rare compared to the memtable push, an optimistic lock is performant enough.

### Read path

![](./resources/read.png)

### Write path

![](./resources/write.png)

### Multi-threading Performance

The hot memtabe is currently guarded by a Mutex, which means read and write need to first grab the lock, if there are multiple user thread that read and write concurrently, it may decrease the performance.

For flush and compact thread, as mentioned before, the optimistic lock is performant enough.

So the best use case is to use a small number of read write thread for a better performance.

### Arc and File Deletion

Each SSTable represent a file on disk, a SSTable is wrapped with an `Arc`, it maybe shared by some version, flush thread, compact thread or user read, when the reference count decrease to zero, which means the fie is no longer needed, the file will be deleted.

## Integration Test

```sh
cargo test -v --test integration_test -- --show-output --test-threads=1
```

## Roadmap

- [x] memtable
- [x] sstable
- [x] multi-threaded read and write
- [x] flush thread
- [x] compaction thread
- [ ] write ahead log
