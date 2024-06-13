# Nessie Server upgrade notes

The following table lists the upgrade types from one Nessie version to another Nessie version.

A check-mark in the _Rolling Upgrade_ column means that it is okay to run Nessie instances running
versions in the _From_ column during the limited time of a rolling-upgrade with Nessie versions
in the _To_ column.

A red cross in the _Rolling Upgrade_ column means that rolling upgrades for the mentioned versions
are not supported and must be avoided.

**Also read the [release notes on GitHub](https://github.com/projectnessie/nessie/releases) and the
[release notes page](releases.md).**

| Rolling Upgrade Supported | _From_ Nessie version | _To_ Nessie version |
|---------------------------|-----------------------|---------------------|
| :heavy_check_mark:        | 0.61.0 or newer       | 0.90.4 or newer     |
| :x:                       | 0.40.0 or newer       | 0.61.0 or newer     |
| :heavy_check_mark:        | 0.40.0 or newer       | 0.60.0 or newer     |
| :x:                       | < 0.40.0              | 0.40.0 or newer     |
| :heavy_check_mark:        | 0.26.0 to 0.29.0      | 0.27.0 to 0.30.0    |
| :x:                       | 0.25.0 or older       | 0.26.0 or newer     |
| :heavy_check_mark:        | 0.18.0 to 0.24.0      | 0.19.0 to 0.25.0    |

Older releases than 0.18.0 are not supported.

See [Releases](releases.md) for release notes.

## Rolling upgrades target version notes

**Also read the [release notes on GitHub](https://github.com/projectnessie/nessie/releases) and the
[release notes page](releases.md).**

### "Legacy" version store types 

**The version store types `ROCKS`, `MONGO`, `DYNAMO`, `TRANSACTIONAL` and `INMEMORY` were deprecated for a long time
and have been removed in Nessie 0.75.0!**

If you are using one of these version types migrate to one of the supported version store type mentioned above
**before** upgrading to Nessie 0.75.0 or newer.

The migration process is described in the [migration guide](guides/migration.md).

### Nessie 0.61.0

The serialized format of internal key-indexes got bumped. For a rolling-upgrade to version 0.61.0 or newer, follow these
steps.

1. Deploy Nessie with the system property `nessie.internal.store-index-format-version` set to `1`
2. Perform the rolling upgrade to 0.61.0 or newer
3. Remove the system property `nessie.internal.store-index-format-version` (or set it to `2`)
4. Perform a rolling restart

Alternatively you can also stop all Nessie instances, upgrade those and just restart.
