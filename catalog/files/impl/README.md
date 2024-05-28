# Nessie Catalog Object-Store I/O

This module provides `ObjectIO` support for S3, GCS and ADLS Gen2, supporting multiple buckets, even on different
storage backends, for example multiple MinIO instances.

The various configuration options for each object store consist of a "base" configuration, and per-bucket
configurations. The base configuration also provides defaults for all buckets.

All object store client implementations use a shared HTTP client, while basically each request uses its own client API
facade (e.g. `S3Client`) that use the shared HTTP client.

## Resource Usage

We have implemented micro-ish-benchmarks to verify that there are no resource (threads, heap, etc) leaks and that the
per-request overhead is negligible.

There are 3 benchmarks for each object store client:

1. `*Client`, which requests a object storage client object (`S3Client` et al) via the `*ClientSupplier`s
2. `*Get`, which issues a GET to a 0-length object

| Object Store Client   | get-client-instance | read 0-length object | read 250k object | read 4M object |
|-----------------------|---------------------|----------------------|------------------|----------------|
| ADLS                  | ~0.17ms ~90kB       | ~2.6ms ~100kB        | ~4.3ms ~1.7MB    | ~75ms ~22MB    |
| GCS                   | ~0.3ms ~64kB        | ~10ms ~2.3MB         | ~6.3ms ~2.8MB    | ~42ms ~11MB    |
| S3 (sync Apache HTTP) | ~0.15ms ~90kB       | ~2.4ms ~200kB        | ~3.3ms ~725kB    | ~31ms ~8.5MB   |

Below are the detailed results from a benchmark ran on 2024/04/03. Used an AMD Ryzen 9 7950X CPU (16 cores, 32 threads),
Java 21.0.2, Linux 6.8.2. Heap size is limited to let potential heap resource leaks show up. The 4M-gets benchmarks
require a heap size of 1g, without those, 200m is sufficient.

Nessie Catalog also supports STS, which uses a local cache - the results are shown via
the `S3SessionCacheResourceBench`. Note that the default cache size is 1000 entries, so it is not surprising, that
the measurements "go up" for more than 1000 different cache keys.

```bash
./gradlew :nessie-catalog-files-impl:jmhJar && \
  java \ 
    -Xmx1g \ 
    -jar catalog/files/impl/build/libs/nessie-catalog-files-impl-0.79.1-SNAPSHOT-jmh.jar \
    -t 32 \
    -i 10 \ 
    -prof gc
```

```
Benchmark                                                                 (numBucketOptions)  Mode  Cnt         Score        Error   Units
o.p.c.f.adls.AdlsClientResourceBench.adlsClient                                          N/A  avgt   10       167.956 ±      2.558   us/op
o.p.c.f.adls.AdlsClientResourceBench.adlsClient:gc.alloc.rate                            N/A  avgt   10     15937.664 ±   4071.613  MB/sec
o.p.c.f.adls.AdlsClientResourceBench.adlsClient:gc.alloc.rate.norm                       N/A  avgt   10     92408.352 ±      0.881    B/op
o.p.c.f.adls.AdlsClientResourceBench.adlsClient:gc.count                                 N/A  avgt   10       519.000               counts
o.p.c.f.adls.AdlsClientResourceBench.adlsClient:gc.time                                  N/A  avgt   10       390.000                   ms
o.p.c.f.adls.AdlsClientResourceBench.adlsGet                                             N/A  avgt   10      2830.302 ±     48.461   us/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet:gc.alloc.rate                               N/A  avgt   10      1700.349 ±    429.386  MB/sec
o.p.c.f.adls.AdlsClientResourceBench.adlsGet:gc.alloc.rate.norm                          N/A  avgt   10    166306.346 ±     19.498    B/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet:gc.count                                    N/A  avgt   10        51.000               counts
o.p.c.f.adls.AdlsClientResourceBench.adlsGet:gc.time                                     N/A  avgt   10       163.000                   ms
o.p.c.f.adls.AdlsClientResourceBench.adlsGet250k                                         N/A  avgt   10      4332.767 ±    123.587   us/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet250k:gc.alloc.rate                           N/A  avgt   10     11862.005 ±   2959.727  MB/sec
o.p.c.f.adls.AdlsClientResourceBench.adlsGet250k:gc.alloc.rate.norm                      N/A  avgt   10   1774122.135 ±   3577.682    B/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet250k:gc.count                                N/A  avgt   10       338.000               counts
o.p.c.f.adls.AdlsClientResourceBench.adlsGet250k:gc.time                                 N/A  avgt   10       656.000                   ms
o.p.c.f.adls.AdlsClientResourceBench.adlsGet4M                                           N/A  avgt   10     75868.348 ±   5571.871   us/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet4M:gc.alloc.rate                             N/A  avgt   10      8480.357 ±   2036.870  MB/sec
o.p.c.f.adls.AdlsClientResourceBench.adlsGet4M:gc.alloc.rate.norm                        N/A  avgt   10  21937498.706 ± 166766.015    B/op
o.p.c.f.adls.AdlsClientResourceBench.adlsGet4M:gc.count                                  N/A  avgt   10       558.000               counts
o.p.c.f.adls.AdlsClientResourceBench.adlsGet4M:gc.time                                   N/A  avgt   10      1395.000                   ms
o.p.c.f.gcs.GcsClientResourceBench.gcsClient                                             N/A  avgt   10       172.315 ±     39.555   us/op
o.p.c.f.gcs.GcsClientResourceBench.gcsClient:gc.alloc.rate                               N/A  avgt   10     10396.643 ±   2948.448  MB/sec
o.p.c.f.gcs.GcsClientResourceBench.gcsClient:gc.alloc.rate.norm                          N/A  avgt   10     61257.282 ±   4096.977    B/op
o.p.c.f.gcs.GcsClientResourceBench.gcsClient:gc.count                                    N/A  avgt   10       183.000               counts
o.p.c.f.gcs.GcsClientResourceBench.gcsClient:gc.time                                     N/A  avgt   10       731.000                   ms
o.p.c.f.gcs.GcsClientResourceBench.gcsGet                                                N/A  avgt   10      6474.466 ±   1724.295   us/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet:gc.alloc.rate                                  N/A  avgt   10     10591.747 ±   3671.968  MB/sec
o.p.c.f.gcs.GcsClientResourceBench.gcsGet:gc.alloc.rate.norm                             N/A  avgt   10   2306788.499 ±   6164.669    B/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet:gc.count                                       N/A  avgt   10      3719.000               counts
o.p.c.f.gcs.GcsClientResourceBench.gcsGet:gc.time                                        N/A  avgt   10      2837.000                   ms
o.p.c.f.gcs.GcsClientResourceBench.gcsGet250k                                            N/A  avgt   10      6382.948 ±   1439.131   us/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet250k:gc.alloc.rate                              N/A  avgt   10     13056.243 ±   3859.919  MB/sec
o.p.c.f.gcs.GcsClientResourceBench.gcsGet250k:gc.alloc.rate.norm                         N/A  avgt   10   2837434.154 ±   1454.798    B/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet250k:gc.count                                   N/A  avgt   10       974.000               counts
o.p.c.f.gcs.GcsClientResourceBench.gcsGet250k:gc.time                                    N/A  avgt   10      1523.000                   ms
o.p.c.f.gcs.GcsClientResourceBench.gcsGet4M                                              N/A  avgt   10     41864.004 ±   8093.163   us/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet4M:gc.alloc.rate                                N/A  avgt   10      7720.587 ±   2124.889  MB/sec
o.p.c.f.gcs.GcsClientResourceBench.gcsGet4M:gc.alloc.rate.norm                           N/A  avgt   10  10893309.743 ±   4771.429    B/op
o.p.c.f.gcs.GcsClientResourceBench.gcsGet4M:gc.count                                     N/A  avgt   10       373.000               counts
o.p.c.f.gcs.GcsClientResourceBench.gcsGet4M:gc.time                                      N/A  avgt   10       909.000                   ms
o.p.c.f.s3.S3ClientResourceBench.s3Get                                                   N/A  avgt   10      2640.906 ±     97.745   us/op
o.p.c.f.s3.S3ClientResourceBench.s3Get:gc.alloc.rate                                     N/A  avgt   10      2266.069 ±    612.552  MB/sec
o.p.c.f.s3.S3ClientResourceBench.s3Get:gc.alloc.rate.norm                                N/A  avgt   10    206253.310 ±     14.822    B/op
o.p.c.f.s3.S3ClientResourceBench.s3Get:gc.count                                          N/A  avgt   10        76.000               counts
o.p.c.f.s3.S3ClientResourceBench.s3Get:gc.time                                           N/A  avgt   10       219.000                   ms
o.p.c.f.s3.S3ClientResourceBench.s3Get250k                                               N/A  avgt   10      3315.931 ±    128.399   us/op
o.p.c.f.s3.S3ClientResourceBench.s3Get250k:gc.alloc.rate                                 N/A  avgt   10      6368.189 ±   1644.113  MB/sec
o.p.c.f.s3.S3ClientResourceBench.s3Get250k:gc.alloc.rate.norm                            N/A  avgt   10    728448.294 ±     17.150    B/op
o.p.c.f.s3.S3ClientResourceBench.s3Get250k:gc.count                                      N/A  avgt   10       194.000               counts
o.p.c.f.s3.S3ClientResourceBench.s3Get250k:gc.time                                       N/A  avgt   10       448.000                   ms
o.p.c.f.s3.S3ClientResourceBench.s3Get4M                                                 N/A  avgt   10     30812.790 ±   2431.420   us/op
o.p.c.f.s3.S3ClientResourceBench.s3Get4M:gc.alloc.rate                                   N/A  avgt   10      8213.774 ±   2039.172  MB/sec
o.p.c.f.s3.S3ClientResourceBench.s3Get4M:gc.alloc.rate.norm                              N/A  avgt   10   8729692.697 ±   4021.467    B/op
o.p.c.f.s3.S3ClientResourceBench.s3Get4M:gc.count                                        N/A  avgt   10       361.000               counts
o.p.c.f.s3.S3ClientResourceBench.s3Get4M:gc.time                                         N/A  avgt   10      1176.000                   ms
o.p.c.f.s3.S3ClientResourceBench.s3client                                                N/A  avgt   10       148.992 ±      2.472   us/op
o.p.c.f.s3.S3ClientResourceBench.s3client:gc.alloc.rate                                  N/A  avgt   10     18057.189 ±   4577.423  MB/sec
o.p.c.f.s3.S3ClientResourceBench.s3client:gc.alloc.rate.norm                             N/A  avgt   10     92922.082 ±      0.772    B/op
o.p.c.f.s3.S3ClientResourceBench.s3client:gc.count                                       N/A  avgt   10       577.000               counts
o.p.c.f.s3.S3ClientResourceBench.s3client:gc.time                                        N/A  avgt   10       601.000                   ms
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials                                      1  avgt   10         0.305 ±      0.007   us/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate                        1  avgt   10     15884.052 ±   3940.927  MB/sec
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate.norm                   1  avgt   10       168.001 ±      0.001    B/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.count                             1  avgt   10       518.000               counts
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.time                              1  avgt   10       300.000                   ms
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials                                    100  avgt   10         0.311 ±      0.010   us/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate                      100  avgt   10     15642.660 ±   4021.870  MB/sec
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate.norm                 100  avgt   10       168.001 ±      0.002    B/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.count                           100  avgt   10       475.000               counts
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.time                            100  avgt   10       270.000                   ms
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials                                   1000  avgt   10         0.297 ±      0.007   us/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate                     1000  avgt   10     16341.422 ±   4092.724  MB/sec
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate.norm                1000  avgt   10       168.266 ±      0.026    B/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.count                          1000  avgt   10       490.000               counts
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.time                           1000  avgt   10       276.000                   ms
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials                                  10000  avgt   10      2470.195 ±     52.801   us/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate                    10000  avgt   10      1487.727 ±    386.185  MB/sec
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate.norm               10000  avgt   10    126611.686 ±    276.483    B/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.count                         10000  avgt   10        50.000               counts
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.time                          10000  avgt   10       166.000                   ms
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials                                 100000  avgt   10      2733.900 ±    177.295   us/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate                   100000  avgt   10      1483.453 ±    395.375  MB/sec
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.alloc.rate.norm              100000  avgt   10    139928.092 ±    189.686    B/op
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.count                        100000  avgt   10        45.000               counts
o.p.c.f.s3.S3SessionCacheResourceBench.getCredentials:gc.time                         100000  avgt   10       165.000                   ms
```
