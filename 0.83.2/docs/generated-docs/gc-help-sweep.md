---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-gc.jar sweep [-hV] [--[no-]defer-deletes]
                           [--allowed-fpp=<allowedFalsePositiveProbability>]
                           [--expected-file-count=<expectedFileCount>]
                           [--expiry-parallelism=<parallelism>] [--fpp=<falsePositiveProbability>]
                           [--max-file-modification=<maxFileModificationTime>]
                           [--time-zone=<zoneId>] [-H=<String=String>[,<String=String>...]]...
                           [-I=<String=String>[,<String=String>...]]... ([--inmemory] | [[--jdbc]
                           --jdbc-url=<url> [--jdbc-properties[=<String=String>[,
                           <String=String>...]...]]... [--jdbc-user=<user>]
                           [--jdbc-password=<password>] [--jdbc-schema=<schemaCreateStrategy>]])
                           (-l=<liveSetId> | -L=<liveSetIdFile>)
Run expire-files + delete-orphan-files phase of Nessie GC using a live-contents-set stored by a
previous run of the mark-live command, must not be used with the in-memory contents-storage.
      --allowed-fpp=<allowedFalsePositiveProbability>
                             The worst allowed effective false-positive-probability checked after
                               the files for a single content have been checked, defaults to 1.0E-4.
      --[no-]defer-deletes   Identified unused/orphan files are by default immediately deleted.
                               Using deferred deletion stores the files to be deleted, so the can
                               be inspected and deleted later. This option is incompatible with
                               --inmemory.
      --expected-file-count=<expectedFileCount>
                             The total number of expected live files for a single content, defaults
                               to 1000000.
      --expiry-parallelism=<parallelism>
                             Number of contents that are checked in parallel.
      --fpp=<falsePositiveProbability>
                             The false-positive-probability used to construct the bloom-filter
                               identifying whether a file is live, defaults to 1.0E-5.
  -h, --help                 Show this help message and exit.
  -H, --hadoop=<String=String>[,<String=String>...]
                             Hadoop configuration option, required when using an Iceberg FileIO
                               that is not S3FileIO.
                             The following configuration settings might be required.

                             For S3:
                             - fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
                             - fs.s3a.access.key
                             - fs.s3a.secret.key
                             - fs.s3a.endpoint, if you use an S3 compatible object store like MinIO

                             For GCS:
                             - fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
                             - fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.
                               GoogleHadoopFS
                             - fs.gs.project.id
                             - fs.gs.auth.type=USER_CREDENTIALS
                             - fs.gs.auth.client.id
                             - fs.gs.auth.client.secret
                             - fs.gs.auth.refresh.token

                             For ADLS:
                             - fs.azure.impl=org.apache.hadoop.fs.azure.AzureNativeFileSystemStore
                             - fs.AbstractFileSystem.azure.impl=org.apache.hadoop.fs.azurebfs.Abfs
                             - fs.azure.storage.emulator.account.name
                             - fs.azure.account.auth.type=SharedKey
                             - fs.azure.account.key.<account>=<base-64-encoded-secret>
  -I, --iceberg=<String=String>[,<String=String>...]
                             Iceberg properties used to configure the FileIO.
                             The following properties are almost always required.

                             For S3:
                             - s3.access-key-id
                             - s3.secret-access-key
                             - s3.endpoint, if you use an S3 compatible object store like MinIO

                             For GCS:
                             - io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO
                             - gcs.project-id
                             - gcs.oauth2.token

                             For ADLS:
                             - io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
                             - adls.auth.shared-key.account.name
                             - adls.auth.shared-key.account.key
      --inmemory             Flag whether to use the in-memory contents storage. Prefer a JDBC
                               storage.
      --inmemory             Flag whether to use the in-memory contents storage. Prefer a JDBC
                               storage.
      --jdbc                 Flag whether to use the JDBC contents storage.
      --jdbc                 Flag whether to use the JDBC contents storage.
      --jdbc-password=<password>
                             JDBC password used to authenticate the database access.
      --jdbc-password=<password>
                             JDBC password used to authenticate the database access.
      --jdbc-properties[=<String=String>[,<String=String>...]...]
                             JDBC parameters.
      --jdbc-properties[=<String=String>[,<String=String>...]...]
                             JDBC parameters.
      --jdbc-schema=<schemaCreateStrategy>
                             How to create the database schema. Possible values: CREATE,
                               DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.
      --jdbc-schema=<schemaCreateStrategy>
                             How to create the database schema. Possible values: CREATE,
                               DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.
      --jdbc-url=<url>       JDBC URL of the database to connect to.
      --jdbc-url=<url>       JDBC URL of the database to connect to.
      --jdbc-user=<user>     JDBC user name used to authenticate the database access.
      --jdbc-user=<user>     JDBC user name used to authenticate the database access.
  -l, --live-set-id=<liveSetId>
                             ID of the live content set.
  -L, --read-live-set-id-from=<liveSetIdFile>
                             The file to read the live-set-id from.
      --max-file-modification=<maxFileModificationTime>
                             The maximum allowed file modification time. Files newer than this
                               timestamp will not be deleted. Defaults to the created timestamp of
                               the live-content-set.
      --time-zone=<zoneId>   Time zone ID used to show timestamps.
                             Defaults to system time zone.
  -V, --version              Print version information and exit.

```
