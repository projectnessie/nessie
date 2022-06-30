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

### IntelliJ IDEA tips

Nothing special for IntelliJ IDEA, just trust the project and let IntelliJ import it as a Gradle
project.

### Gradle tips

Common Gradle tasks:

* Check whether everything compiles (no style checks):
  `./gradlew jar testClasses`
* Automatically fix code style issues:
  `./gradlew spotlessApply` (abbreviated: `sAp`)
* Publish to local Maven repo:
  `./gradlew publishToLocalMaven` (abbreviated: `pTML`)
* Run unit tests:
  `./gradlew test`, also `./gradlew build`
* Run integration tests:
  `./gradlew intTest`
* Run all checks (including tests):
  `./gradlew check`

It is fine to just run all `test` tasks, Gradle will only execute a task, when anything that the
task depends on has been changed.

#### Abbreviations

You can abbreviate project and task names,
see [docs](https://docs.gradle.org/current/userguide/command_line_interface.html#sec:name_abbreviation):
* For `./gradlew spotlessApply` you can write `./gradlew sAp`
* For `./gradlew :nessie-versioned-persist-serialize-proto:tasks` you can write `./gradle :n-v-p-s-p:tasks`

#### Local Maven repository

Using the local Maven repository is [discouraged](https://docs.gradle.org/current/userguide/declaring_repositories.html#sec:case-for-maven-local).

If you really have to use the local Maven repository, you can use it by explicitly instructing
the build to do so by passing `-DwithMavenLocal=true`. Be aware that Gradle does *not* cache
anything from the local Maven repository and builds will be significantly slower.

Note: `-DwithMavenLocal=true` allows using the local Maven repository for *dependencies*. This is
different from the Gradle task `publishToMavenLocal`, which *publishes* the Nessie artifacts to
the local Maven repository.

#### Improving the build time for native images

The native image builds require quite some amount of Java heap for the native image compilation.
The value can be bumped by putting for example the following value to your local
`~/.gradle/gradle.properties` file:

```properties
quarkus.native.native-image-xmx=8g
```

#### Heap size and JVM arguments for tests

Tests run with the default Java heap size, which is sufficient for all tests. If you really need to
bump the heap size for tests, for example during development, you can do so via the _project_ property
`testHeapSize`. For example: `./gradlew -PtestHeapSize=4g :nessie-client:test --tests TestMyStuff`

The project property `testJvmArgs` allows specifying JVM arguments for tests. Example:
`./gradlew -PtestJvmArgs="-Xmx8g -XX:+UnlockExperimentalVMOptions -XX:+UseZGC" :nessie-client:test --tests TestMyStuff`.
Multiple JVM arguments can be specified via the `testJvmArgs` property, separated by spaces.

Note: if you need these JVM settings regularly, you can also specify those in the
`~/.gradle/gradle.properties` file or set those via the environment, for example via
`export ORG_GRADLE_PROJECT_testHeapSize=4g`.

#### `javadoc` task fail

If the `javadoc` task fails, check that there's a `javadoc` executable. If not, install a "full" JDK.

#### Migrating an existing Nessie clone using Maven

In June 2022 the Nessie code tree changed to the Gradle build tool. Existing clones using Maven
can be migrated as follows:

1. Close your IDE
2. Run `git clean -xdf`
3. Run `git pull`
4. Run `./gradlew testClasses` to ensure the build works fine. The very first build will be slower,
   because it has to assemble the build plugins and download dependencies.
6. Make sure that your IDE has no more "references to Maven" (nothing to do when using IntelliJ) 
7. Open Nessie in your IDE

Note: The Gradle build does *not* use the local Maven repository and dependencies there will not be
used by default. See [Local Maven reposotiry](#local-maven-repository).

### Development process

The development process doesn't contain many surprises. As most projects on github anyone can contribute by
forking the repo and posting a pull request. See 
[GitHub's documentation](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) 
for more information. Small changes don't require an issue. However, it is good practice to open up an issue for
larger changes. If you are unsure of where to start ask on the slack channel or look at [existing issues](https://github.com/projectnessie/nessie/issues).
The [good first issue](https://github.com/projectnessie/nessie/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label marks issues that are particularly good for people new to the codebase.

For the Spark tests to run with Java 16 or newer, you need to have Java 11 installed. Gradle will
most likely find the Java 11 runtime required to run the Spark tests. Run `./gradlew javaToolchains`
to see the Java toolchains that Gradle discovered. If Gradle could not locate your Java 11 runtime,
consult the [docs](https://docs.gradle.org/current/userguide/toolchains.html).

#### Building with Java 17 (and 16)

Due to [JEP 396](https://openjdk.java.net/jeps/396), introduced in Java 16, a couple JVM options are required for
[google-java-format](https://github.com/google/google-java-format#jdk-16) and [errorprone](https://errorprone.info/docs/installation)
to work. These options are harmless when using Java 11.

Apache Spark does **only** work with Java 11 (or 8), so all tests using Spark use the Gradle toolchain mechanism
to force Java 11 for the execution of those tests.

### Style guide

Changes must adhere to the style guide and this will be verified by the continuous integration build.

* Java code style is [Google style](https://google.github.io/styleguide/javaguide.html).
* Kotlin code style is [ktfmt w/ Google style](https://github.com/facebookincubator/ktfmt#ktfmt-vs-ktlint-vs-intellij).
* Scala code style is [scalafmt](https://scalameta.org/scalafmt/).
* Python adheres to the pep8 standard.

Java and Scala code style is checked by [Spotless](https://github.com/diffplug/spotless)
with [google-java-format](https://github.com/google/google-java-format) and
[scalafmt](https://scalameta.org/scalafmt/) during build.

Python code style is checked by flake8/black.

#### Automatically fixing code style issues

Java, Scala and Kotlin code style issues can be fixed from the command line using
`./gradlew spotlessApply`.

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

Upon submission of a pull request you will be asked to sign our contributor license agreement.
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
