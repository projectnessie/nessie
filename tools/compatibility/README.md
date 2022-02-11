# Nessie Compatibility Tests

The Nessie project has three different kinds of tests to validate backwards compatibility:

* API compatibility: Ensure that the current (in-source-tree) Nessie client implementation works
  against older, already released Nessie server versions. These tests are annotated with
  `@ExtendWith(OlderNessieServersExtension.class)`.
* API compatibility: Ensure that the current (in-source-tree) Nessie server implementation works
  with older, already released Nessie client versions. These tests are annotated with
  `@ExtendWith(OlderNessieClientsExtension.class)`.
* Persisted data compatibility: run a series of tests against different versions of Nessie server
  plus Nessie client. Starts with the configured "oldest" versions, runs the tests, stops server +
  client, starts Nessie server + client with the "next" version, runs the tests, and so on. The
  "last" version that is tested is the current version (in source-tree). These tests are annotated
  with `@ExtendWith(NessieUpgradesExtension.class)`.

## Compatibility tests - things to know

All (most) JUnit features and extensions should just work on compatibility tests. Test ordering,
parameterized tests, repeated tests, etc. should just work.

Each compatibility test class is executed once per tested Nessie version. This means, that a single
compatibility test class is meant to be executed more than once.

The `@BeforeAll` and `@AfterAll` annotated methods are executed before and after a test class
*per exercised Nessie version*.

## Test classes

The test classes for all kinds of tests are rather standard JUnit 5 test classes.

Custom annotations:

* `@Nessie` is used on fields to get the currently tested `NessieApiV1` injected. Example:
  ```java
  class ITMyTest {
    @Nessie NessieApiV1 nessieApiInstance; // can also be static
  }
  ```
* `@NessieVersion` is used on fields to get the currently tested `Version` injected. Example:
  ```java
  class ITMyTest {
    @NessieVersion Version currentlyExercisedNessieVersion; // can also be static
  }
  ```
* `@NessieVersions` is used to restrict certain test methods or classes to run only with a minimum
  and/or maximum Nessie version. Example:
  ```java
  class ITMyTest {
    @Test
    @NessieVersions(minVersion = "0.15")
    void doesNotRunForNessieOlder_0_dot_15() {
      // use something that's new in Nessie 0.15
    }
  }
  ```
  ```java
  class ITMyTest {
    @Test
    @NessieVersions(maxVersion = "0.19")
    void doesNotRunForNessieNewer_0_dot_19() {
      // use something that's been removed in Nessie 0.19
    }
  }
  ```
  ```java
  class ITMyTest {
    @Test
    @NessieVersions(maxVersion = "not-current")
    void doesNotRunForInTreeNessie() {
      // Useful for upgrade test scenarios, when you do not want to run a test against the current
      // in-tree version.
    }
  }
  ```
  ```java
  class ITMyTest {
    @Test
    @NessieVersions(minVersion = "current")
    void onlyRunsForTheInTreeNessieCode() {
      // Runs only for the current, in-tree Nessie
    }
  }
  ```
  ```java
  @NessieVersions(minVersion = "0.20")
  class ITMyTest {
    // all tests only run for Nessie >= 0.20
  }
  ```

## Version numbers

Nessie releases are referred to using their release versions, for example `0.15.0` or `0.17.123`.

The current in-tree version is referred to using the version string `current`. There is also a
"special" version `not-current` that's useful in the `@NessieVersions` annotation to exclude the
in-tree version.

Note that trailing `.0`s "do not matter", so `0.15` and `0.15.0` are semantically equal.

## Nessie releases exercised by the tests

The list of Nessie versions is taken from the system property `nessie.versions`. If that does not
exist, it's read from the resource
[`META-INF/nessie-compatibility.properties`](compatibility-tests/src/test/resources/META-INF/nessie-compatibility.properties)
.

`nessie.versions` refers to a set of release versions, and the `current` version should be included.
The order of the versions does not really matter, because that list will be sorted by the
implementation.

## Nessie server and API instance lifecycle

Generated instances of the Nessie server and instances of `NessieApi` (e.g. `NessieApiV1`) are
maintained per test class.

## Maven modules

* [`:nessie-compatibility-common`](./common) contains common code for JUnit engine and extensions,
  running current and already released Nessie servers, using current and already released Nessie
  clients.
* [`:nessie-compatibility-tests`](./compatibility-tests) contains shared tests for
  `:nessie-client-backward-compatibility` and `:nessie-server-backward-compatibility`.
* [`:nessie-compatibility-jersey`](./jersey) Helper code to run older Nessie servers. It's a
  separate module to keep the huge amount of dependency declarations separate from
  `:nessie-compatibility-common`.

## API compatibility implementation

Tests that use old Nessie client implementations expose the current in-tree API to the test code.
Invocations on the current in-tree API are translated using `java.lang.reflect.Proxy` to invoke the
old Nessie API implementation. Model objects are re-serialized using two Jackson instances, one that
is constructed using the application class loader and one that is constructed using a class loader
for the exercised old Nessie version.
