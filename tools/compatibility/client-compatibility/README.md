Compatibility checks to verify old Nessie clients
=================================================

Contains configuration and code to exercise Nessie clients from previous Nessie releases against
the current Nessie server implementation.

In the `pom.xml` of this module, update the `nessieClientVersions` property in the
`maven-failsafe-plugin` declaration to contain one or more Nessie version numbers. For example:
`<nessieClientVersions>0.17.0</nessieClientVersions>` or
`<nessieClientVersions>0.16.0, 0.17.0</nessieClientVersions>`.

The implementation uses a custom JUnit engine implementation that delegates to a "normal" JUnit 5
engine. It does this using a dynamically constructed test class that extends `AbstractTestRest` and
`AbstractResteasyTest` using the dependencies and classpath for the currently exercised Nessie client
version in an isolated classloader, using the artifact
`org.projectnessie:nessie-jaxrs-tests:<nessieVersionToTest>`.

Test reports are properly generated.
