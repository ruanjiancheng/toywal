wal 机制是一种用于持久化操作保证数据可靠性的机制。当数据保存在内存中的时候，需要使用 wal 机制来保证数据可靠性。

由于 wal 通过 append-only 顺序写入来持久化操作，使得 wal 具有较高写入性能。

当出现故障的时候，系统可以通过加载 wal 中的操作来恢复内存中的数据。在很多场景中都会用到 wal，例如 LSM Tree 维护内存表，InnoDB 的 redo log。

## golang 实现 wal

将 wal 分为 wal、segment、block 以及 chunk。一个 chunk 表示一条记录，包括 header。一个 block 大小默认是 32kb。

golang 操作底层文件：
```golang
s.fd.Write(data)

s.fd.ReadAt(data, offset)
```

- 写入文件的时候，先写入 bytebufferpool，然后再写入 buffer 中的内容写入 disk。
- 读取文件的时候，读入 []byte 中，再使用 binary 库中的函数进行解析

