# Nessie/Iceberg for tools-integration-tests

The Gradle in this directory is purely intended to be included by the tools-integration-tests.
It helps to provide a clean "hierarchy" (or sequence) of included Gradle builds, which are:
1. Nessie build, without projects that depend on Apache Iceberg
2. Apache Iceberg build, depends on Nessie
3. Nessie/Iceberg build (this one) that depends on Nessie and Iceberg

This Gradle build is **not** meant to be run as a "standalone" build, hence there is no
`gradlew` executable.

Necessary files that are equal to the ones in the main Nessie build are not copied but
symlinked. Hint: when editing files in these files/directories, do it in the "original"
location - otherwise it might confuse IDEs.
* `codestyle/`
* `gradle/`

`gradle.properties` is a duplicate of the one in the main Nessie build, with some additions.
