---
search:
  exclude: true
---
<!--start-->

When setting `nessie.version.store.type=BIGTABLE` which enables Google BigTable as the  version store used by the Nessie server, the following configurations are applicable.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.bigtable.instance-id` | `nessie` | `String` | Sets the instance-id to be used with Google BigTable.  |
| `nessie.version.store.persist.bigtable.emulator-port` | `8086` | `int` | When using the BigTable emulator, used to configure the port.  |
| `nessie.version.store.persist.bigtable.enable-telemetry` | `true` | `boolean` | Enables telemetry with OpenCensus.  |
| `nessie.version.store.persist.bigtable.table-prefix` |  | `String` | Prefix for tables, default is no prefix.  |
| `nessie.version.store.persist.bigtable.no-table-admin-client` | `false` | `boolean` |  |
| `nessie.version.store.persist.bigtable.app-profile-id` |  | `String` | Sets the profile-id to be used with Google BigTable.  |
| `nessie.version.store.persist.bigtable.quota-project-id` |  | `String` | Google BigTable quote project ID (optional).  |
| `nessie.version.store.persist.bigtable.endpoint` |  | `String` | Google BigTable endpoint (if not default).  |
| `nessie.version.store.persist.bigtable.mtls-endpoint` |  | `String` | Google BigTable MTLS endpoint (if not default).  |
| `nessie.version.store.persist.bigtable.emulator-host` |  | `String` | When using the BigTable emulator, used to configure the host.  |
| `nessie.version.store.persist.bigtable.jwt-audience-mapping.`_`<mapping>`_ |  | `String` | Google BigTable JWT audience mappings (if necessary).  |
| `nessie.version.store.persist.bigtable.initial-retry-delay` |  | `Duration` | Initial retry delay.  |
| `nessie.version.store.persist.bigtable.max-retry-delay` |  | `Duration` | Max retry-delay.  |
| `nessie.version.store.persist.bigtable.retry-delay-multiplier` |  | `double` |  |
| `nessie.version.store.persist.bigtable.max-attempts` |  | `int` | Maximum number of attempts for each Bigtable API call (including retries).  |
| `nessie.version.store.persist.bigtable.initial-rpc-timeout` |  | `Duration` | Initial RPC timeout.  |
| `nessie.version.store.persist.bigtable.max-rpc-timeout` |  | `Duration` |  |
| `nessie.version.store.persist.bigtable.rpc-timeout-multiplier` |  | `double` |  |
| `nessie.version.store.persist.bigtable.total-timeout` |  | `Duration` | Total timeout (including retries) for Bigtable API calls.  |
| `nessie.version.store.persist.bigtable.min-channel-count` |  | `int` | Minimum number of gRPC channels. Refer to Google docs for details. |
| `nessie.version.store.persist.bigtable.max-channel-count` |  | `int` | Maximum number of gRPC channels. Refer to Google docs for details. |
| `nessie.version.store.persist.bigtable.initial-channel-count` |  | `int` | Initial number of gRPC channels. Refer to Google docs for details |
| `nessie.version.store.persist.bigtable.min-rpcs-per-channel` |  | `int` | Minimum number of RPCs per channel. Refer to Google docs for details. |
| `nessie.version.store.persist.bigtable.max-rpcs-per-channel` |  | `int` | Maximum number of RPCs per channel. Refer to Google docs for details. |
