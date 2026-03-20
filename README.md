# MossDB

<img width="100" alt="mossdb" src="./resources/logo.png" />

- [ ] update the final architechture
- [ ] the current arch
- [ ] road map & todos

## Architecture

main thread

- user command: set get del ... read memtable and sstable, write memtable
- when memtable reaches limit, swap with a new memtable, and hand over the old memtable to the flush thread
- accept notification from write thread when memtable is flushed to disk, it then update sstable
- accept notification from compaction thread when compaction finished, the old log file still exists, main thread update with the newly compacted sstable, and delete the old logs

flush thread

- long running thread, used to flush the old memtable to disk log
- when finished flushing, notify main thread and compaction thread with the namme of new log file

compaction thread

- long running thread
- notified when new log file is created, scan the log dir and try to compact log files
- when finishing compaction, notify main thread
- doesn't delete log file, only create
