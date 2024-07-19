```
Usage: nessie-gc.jar gc [-hV] [--[no-]defer-deletes]
                        [--allowed-fpp=<allowedFalsePositiveProbability>]
                        [-c=<defaultCutoffPolicy>] [--expected-file-count=<expectedFileCount>]
                        [--expiry-parallelism=<parallelism>] [--fpp=<falsePositiveProbability>]
                        [--identify-parallelism=<parallelism>]
                        [--max-file-modification=<maxFileModificationTime>]
                        [--nessie-api=<nessieApi>] [--nessie-client=<nessieClientName>]
                        [-R=<cutoffPolicyRefTime>] [--time-zone=<zoneId>] [-u=<nessieUri>]
                        [--write-live-set-id-to=<liveSetIdFile>] [-H=<String=String>[,
                        <String=String>...]]... [-I=<String=String>[,<String=String>...]]... [-C
                        [=<String=String>[,<String=String>...]...]]... [-o[=<String=String>[,
                        <String=String>...]...]]... ([--inmemory] | [[--jdbc] --jdbc-url=<url>
                        [--jdbc-properties[=<String=String>[,<String=String>...]...]]...
                        [--jdbc-user=<user>] [--jdbc-password=<password>]
                        [--jdbc-schema=<schemaCreateStrategy>]])
Run identify-live-content and expire-files + delete-orphan-files.
This is the same as running a 'mark-live' + a 'sweep' command, but this variant works with the
in-memory contents storage.
      --allowed-fpp=<allowedFalsePositiveProbability>
                             The worst allowed effective false-positive-probability checked after
                               the files for a single content have been checked, defaults to 1.0E-4.
  -c, --default-cutoff=<defaultCutoffPolicy>
                             Default cutoff policy. Policies can be one of:
                             - number of commits as an integer value
                             - a duration (see java.time.Duration)
                             - an ISO instant
                             - 'NONE', means everything's considered as live
  -C, --cutoff[=<String=String>[,<String=String>...]...]
                             Cutoff policies per reference names. Supplied as a
                               ref-name-pattern=policy tuple.
                             Reference name patterns are regular expressions.
                             Policies can be one of:
                             - number of commits as an integer value
                             - a duration (see java.time.Duration)
                             - an ISO instant
                             - 'NONE', means everything's considered as live
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
      --identify-parallelism=<parallelism>
                             Number of Nessie references that can be walked in parallel.
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
      --max-file-modification=<maxFileModificationTime>
                             The maximum allowed file modification time. Files newer than this
                               timestamp will not be deleted. Defaults to the created timestamp of
                               the live-content-set.
      --nessie-api=<nessieApi>
                             Class name of the NessieClientBuilder implementation to use, defaults
                               to HttpClientBuilder suitable for REST. Using this parameter is not
                               recommended. Prefer the --nessie-client parameter instead.
      --nessie-client=<nessieClientName>
                             Name of the Nessie client to use, defaults to HTTP suitable for REST.
  -o, --nessie-option[=<String=String>[,<String=String>...]...]
                             Parameters to configure the NessieClientBuilder.
  -R, --cutoff-ref-time=<cutoffPolicyRefTime>
                             Reference timestamp for durations specified for --cutoff. Defaults to
                               'now'.
      --time-zone=<zoneId>   Time zone ID used to show timestamps.
                             Defaults to system time zone.
  -u, --uri=<nessieUri>      Nessie API endpoint URI, defaults to http://localhost:19120/api/v2.
  -V, --version              Print version information and exit.
      --write-live-set-id-to=<liveSetIdFile>
                             Optional, the file name to persist the created live-set-id to.

```
