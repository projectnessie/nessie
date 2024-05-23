When setting `nessie.version.store.type=ROCKSDB` which enables RocksDB as the version store  used by the Nessie server, the following configurations are applicable.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.rocks.database-path` | `/tmp/nessie-rocksdb-store` | `Path` | Sets RocksDB storage path.  |
