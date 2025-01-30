```
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.tcp-keep-alive" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.connection-max-idle-time" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.event-loop.override" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.read-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.max-pending-connection-acquires" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,532 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.advanced.use-future-completion-thread-pool" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.proxy.enabled" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.max-concurrency" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.write-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.use-idle-connection-reaper" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.connection-acquisition-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-30 09:11:01,533 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.protocol" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
Usage: nessie-server-admin-tool-runner.jar import [-ehV]
       [--commit-batch-size=<commitBatchSize>]
       [--input-buffer-size=<inputBufferSize>] -p=<import-from>
Imports a Nessie repository from the local file system.
      --commit-batch-size=<commitBatchSize>
                             Batch size when writing commits, defaults to 20.
  -e, --erase-before-import  Erase an existing repository before the import is
                               started.
                             This will delete all previously existing Nessie
                               data.
                             Using this option has no effect, if the Nessie
                               repository does not already exist.
  -h, --help                 Show this help message and exit.
      --input-buffer-size=<inputBufferSize>
                             Input buffer size, defaults to 32768.
  -p, --path=<import-from>   The ZIP file or directory to read the export from.
                             If this parameter refers to a file, the import
                               will assume that it is a ZIP file, otherwise a
                               directory.
  -V, --version              Print version information and exit.

```
