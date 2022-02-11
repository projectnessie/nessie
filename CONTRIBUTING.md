# Contributing to Nessie
## How to contribute
Everyone is encouraged to contribute to the Nessie project. We welcome of course code changes, 
but we are also grateful for bug reports, feature suggestions, helping with testing and 
documentation, or simply spreading the word about Nessie.

There are several ways to get in touch with other contributors:
 * Slack: get an invite to the channel by emailing slack-subscribe@projectnessie.org
 * Google Groups: You can join the discussion at https://groups.google.com/g/projectnessie

More information are available at https://projectnessie.org/develop/

## Code of conduct
You must agree to abide by the Project Nessie [Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting issues
Issues can be filed on GitHub. Please use the template and add as much detail as possible. Including the 
version of the client and server, how the server is being run (eg docker image) etc. The more the community 
knows the more it can help :-)

### Feature Requests

If you have a feature request or questions about the direction of the project please join the slack channel
and ask there. It helps build a richer discussion and more people can be involved than when posting as an issue.

### Large changes or improvements

We are excited to accept new contributors and larger changes. Please join the mailing list and post a proposal 
before submitting a large change. This helps avoid double work and allows the community to arrive at a consensus
on the new feature or improvement.

## Code changes

### Maven tips

A `./mvnw --threads 1C clean install` runs basically "everything" except release/deployment stuff. This is often
not necessary. Some tips:

* `./mvnw --threads 1C package -Dquickly` Just compiles code, no tests, does not build code under `ui/` and `perftest/`.
* `./mvnw --threads 1C package -DskipTests` Compiles everything, runs no tests.
* `./mvnw --threads 1C package -DskipITs` Compiles everything, runs unit tests, but no integration tests.

### Development process

The development process doesn't contain many surprises. As most projects on github anyone can contribute by
forking the repo and posting a pull request. See 
[GitHub's documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) 
for more information. Small changes don't require an issue. However, it is good practice to open up an issue for
larger changes. If you are unsure of where to start ask on the slack channel or look at [existing issues](https://github.com/projectnessie/nessie/issues).
The [good first issue](https://github.com/projectnessie/nessie/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label marks issues that are particularly good for people new to the codebase.

For the Spark tests to run with Java 16 or newer, you need to update your `~/.m2/toolchains.xml` to contain a reference to Java 11. 
```xml
<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>11</version>
      <vendor>sun</vendor>
    </provides>
    <configuration>
      <jdkHome>PATH_TO_YOUR_JAVA_11_HOME</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
```

#### Building with Java 17 (and 16)

Due to [JEP 396](https://openjdk.java.net/jeps/396), introduced in Java 16, a couple JVM options are required for
[google-java-format](https://github.com/google/google-java-format#jdk-16) and [errorprone](https://errorprone.info/docs/installation)
to work. These options are harmless when using Java 11.

Apache Spark does **only** work with Java 11 (or 8), so all tests using Spark use the Maven toolchain mechanism
to force Java 11 for the execution of those tests.

Maven Wrapper, Maven and Maven Daemon automatically pick up the necessary JVM options from `.mvn/jvm.config` or `.mvn/mvnd.properties`.

### Style guide

Changes must adhere to the style guide and this will be verified by the continuous integration build.

* Java code style is [Google style](https://google.github.io/styleguide/javaguide.html).
* Scala code style is [scalafmt](https://scalameta.org/scalafmt/).
* Python adheres to the pep8 standard.

Java and Scala code style is checked by [Spotless](https://github.com/diffplug/spotless)
with [google-java-format](https://github.com/google/google-java-format) and
[scalafmt](https://scalameta.org/scalafmt/) during build.

Python code style is checked by flake8/black.

#### Automatically fixing code style issues

Java and Scala code style issues can be fixed from the command line using
`./mvnw spotless:apply`.

Python code style issues can be fixed from the command line using
```bash
cd python/

[ ! -d venv/ ] && virtualenv venv
. venv/bin/activate
pip install -U -r requirements_lint.txt
 
black pynessie tests
```

#### Configuring the Code Formatter for Intellij IDEA and Eclipse

Follow the instructions for [Eclipse](https://github.com/google/google-java-format#eclipse) or
[IntelliJ](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides),
note the required manual actions for IntelliJ.

#### Code coverage

Code coverage is measured using jacoco plus codecov. The [aggregator-project](./code-coverage)
contains all modules that shall be measured. New modules must be added there as a dependency as well.

### Submitting a pull request

Upon submission of a pull request you will be asked to sign our contributor license agreement. We use [Reviewable.io](https://reviewable.io/) for reviews.
Anyone can take part in the review process and once the community is happy and the build actions are passing a Pull Request will be merged. Support 
must be unanimous for a change to be merged.

All pull-requests automatically trigger CI runs. Two long-running parts of the CI workflow are
skipped for PRs by default, but can be enabled using "labels" on the PR.
* Quarkus native image generation + tests against the native image. The label `pr-native` label enables this.
  The label `pr-native` label enables this, CI results do not appear as a separate job, because
  those run as part of the "Java/Maven" workflow job.
* Nessie-client tests against various combinations of Jackson versions.
  The label `pr-jackson` label enables this and CI result will appear as a separate check.

### Reporting security issues

Please see our [Security Policy](SECURITY.md)
