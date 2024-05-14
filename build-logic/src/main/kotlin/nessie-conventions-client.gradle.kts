/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Java project, production code built for Java 8

plugins {
  `java-library`
  `maven-publish`
  signing
  id("nessie-common-base")
  id("nessie-common-src")
  id("nessie-java")
  id("nessie-testing")
}

tasks.withType<JavaCompile>().configureEach {
  if (path.startsWith(":nessie-client:")) {
    // :nessie-client is a little special, because it contains a Java 11 version for
    // Nessie's dependency-free http client, so setting "release=8", would not work.
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
  } else if (name == "compileJava") {
    // Client production code must be compatible with Java 8
    options.release = 8
  } else {
    // Test code may depend on server code for in-JVM tests, so need Java 11 for test code
    options.release = 11
  }
}
