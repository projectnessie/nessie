# Nessie Server upgrade notes

The following table lists the upgrade types from one Nessie version to another Nessie version.

A check-mark in the _Rolling Upgrade_ column means that it is okay to run Nessie instances running
versions in the _From_ column during the limited time of a rolling-upgrade with Nessie versions
in the _To_ column.

A red cross in the _Rolling Upgrade_ column means that rolling upgrades for the mentioned versions
are not supported and must be avoided.

| Rolling Upgrade Supported | _From_ Nessie version | _To_ Nessie version |
|---------------------------|-----------------------|---------------------|
| :heavy_check_mark:        | 0.40.0 or newer       | 0.60.0 or newer     |
| :x:                       | < 0.40.0              | 0.40.0 or newer     |
| :heavy_check_mark:        | 0.26.0 to 0.29.0      | 0.27.0 to 0.30.0    |
| :x:                       | 0.25.0 or older       | 0.26.0 or newer     |
| :heavy_check_mark:        | 0.18.0 to 0.24.0      | 0.19.0 to 0.25.0    |

Older releases than 0.18.0 are not supported.

See [Releases](releases.md) for release notes.
