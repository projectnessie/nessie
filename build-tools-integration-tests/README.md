# Maven + Gradle integration test project

This project is meant to verify that the artifacts produced by the Nessie build work in both
Maven and Gradle builds that depend on Nessie.

This project is *not* a (useful) standalone project nor does it serve any production code.

## Running the Maven + Gradle build integration tests

1. From the Nessie code tree root directory, run `./gradlew publishToMavenLocal`
2. Then run `./gradlew buildToolsIntegrationTest`

Although it would be possible to make the build tools integration tests depend on all
`publishToMavenLocal` tasks, it would require a bunch of build code, so leaving it as two
Gradle build invocations for now.
