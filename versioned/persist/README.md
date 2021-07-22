# Nessie Version Store Persitence layer

Nessie's Version-Store implementation consists of an implementation
of [VersionStore](../spi/src/main/java/org/projectnessie/versioned/VersionStore.java)
that has all the logic plus a variety of non-transactional and transactional database-adapters that
only perform reads and writes and do not implement any logic.

See [commit kernel](../../site/docs/develop/kernel.md).
