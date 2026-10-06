# SQLite provider notes

## Single-node and local development

SQLite works well for **one machine**, **one writer process** (or coordinated workers with WAL + `busy_timeout`), and **local disk** storage.

## Do not use on network filesystems

**Do not place the SQLite job database on NFS, SMB/CIFS, or other network-mounted volumes** for production job storage. SQLite relies on POSIX advisory locking and local filesystem semantics; network shares often break locking and can cause `database is locked` errors or corruption under concurrent writers.

Use **PostgreSQL** or **MySQL** when multiple app nodes or workers need durable queues on shared infrastructure.

## Multi-process claiming

Concurrent **OS processes** on the same `.db` file contend on SQLite’s single-writer model. Throughput scales differently than PostgreSQL `SKIP LOCKED` across nodes. See `SqliteMultiProcessTests` and `tools/DotnetJobKit.SqliteMultiProcessWorker` for measured multi-process claim rates on your hardware.
