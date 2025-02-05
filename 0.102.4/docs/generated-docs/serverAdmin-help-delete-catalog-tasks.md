---
search:
  exclude: true
---
<!--start-->

```
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.tcp-keep-alive" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.connection-max-idle-time" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.event-loop.override" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.read-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.max-pending-connection-acquires" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,798 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.advanced.use-future-completion-thread-pool" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.proxy.enabled" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.max-concurrency" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.write-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.use-idle-connection-reaper" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.connection-acquisition-timeout" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
2025-01-31 21:51:51,799 WARN  [io.qua.config] (main) Unrecognized configuration key "quarkus.dynamodb.async-client.protocol" was provided; it will be ignored; verify that the dependency extension for this configuration is set or that you did not make a typo
Usage: nessie-server-admin-tool-runner.jar delete-catalog-tasks [-hV]
       [-B=<batchSize>] [-H=<hash>] [-r=<ref>] [-k=<keyElements>]...
       [-s=<statuses>]...
Delete persisted state of Iceberg snapshot loading tasks previously executed by
the Nessie Catalog.
  -B, --batch=<batchSize>   The max number of task IDs to process at the same
                              time.
  -h, --help                Show this help message and exit.
  -H, --hash=<hash>         Commit hash to use (defaults to the HEAD of the
                              specified reference).
  -k, --key-element=<keyElements>
                            Elements or a specific content key to process (zero
                              or more). If not set, all current keys will get
                              their snapshot tasks expired.
  -r, --ref=<ref>           Reference name to use (default branch, if not set).
  -s, --task-status=<statuses>
                            Delete tasks having these statuses (zero or more).
                              If not set, only failed tasks for matching
                              content objects are deleted.
  -V, --version             Print version information and exit.

```
