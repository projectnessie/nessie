/*
 * Copyright (C) 2022 Dremio
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

plugins {
  `java-platform`
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Bill of Materials (BOM)"

dependencies {
  constraints {
    api(rootProject)
    api(project(":nessie-client"))
    api(project(":nessie-compatibility-common"))
    api(project(":nessie-compatibility-tests"))
    api(project(":nessie-compatibility-jersey"))
    api(project(":nessie-gc-base"))
    api(project(":nessie-gc-repository-jdbc"))
    api(project(":nessie-model"))
    api(project(":nessie-jaxrs"))
    api(project(":nessie-jaxrs-testextension"))
    api(project(":nessie-jaxrs-tests"))
    api(project(":nessie-quarkus-common"))
    api(project(":nessie-quarkus-cli"))
    api(project(":nessie-quarkus"))
    api(project(":nessie-quarkus-tests"))
    api(project(":nessie-rest-services"))
    api(project(":nessie-services"))
    api(project(":nessie-server-store"))
    api(project(":nessie-server-store-proto"))
    api(project(":nessie-content-generator"))
    api(project(":nessie-protobuf-relocated"))
    api(project(":nessie-ui"))
    api(project(":nessie-versioned-persist-adapter"))
    api(project(":nessie-versioned-persist-bench"))
    api(project(":nessie-versioned-persist-dynamodb"))
    api(project(":nessie-versioned-persist-dynamodb-test"))
    api(project(":nessie-versioned-persist-in-memory"))
    api(project(":nessie-versioned-persist-in-memory-test"))
    api(project(":nessie-versioned-persist-mongodb"))
    api(project(":nessie-versioned-persist-mongodb-test"))
    api(project(":nessie-versioned-persist-non-transactional"))
    api(project(":nessie-versioned-persist-non-transactional-test"))
    api(project(":nessie-versioned-persist-rocks"))
    api(project(":nessie-versioned-persist-rocks-test"))
    api(project(":nessie-versioned-persist-serialize"))
    api(project(":nessie-versioned-persist-serialize-proto"))
    api(project(":nessie-versioned-persist-store"))
    api(project(":nessie-versioned-persist-tests"))
    api(project(":nessie-versioned-persist-testextension"))
    api(project(":nessie-versioned-persist-transactional"))
    api(project(":nessie-versioned-persist-transactional-test"))
    api(project(":nessie-versioned-spi"))
    api(project(":nessie-versioned-storage-batching"))
    api(project(":nessie-versioned-storage-cache"))
    api(project(":nessie-versioned-storage-cassandra"))
    api(project(":nessie-versioned-storage-common"))
    api(project(":nessie-versioned-storage-common-proto"))
    api(project(":nessie-versioned-storage-common-serialize"))
    api(project(":nessie-versioned-storage-common-tests"))
    api(project(":nessie-versioned-storage-dynamodb"))
    api(project(":nessie-versioned-storage-inmemory"))
    api(project(":nessie-versioned-storage-jdbc"))
    api(project(":nessie-versioned-storage-mongodb"))
    api(project(":nessie-versioned-storage-rocksdb"))
    api(project(":nessie-versioned-storage-spanner"))
    api(project(":nessie-versioned-storage-store"))
    api(project(":nessie-versioned-storage-telemetry"))
    api(project(":nessie-versioned-storage-testextension"))
    api(project(":nessie-versioned-tests"))
    api(project(":nessie-versioned-transfer-proto"))
    api(project(":nessie-versioned-transfer"))
    if (!isIntegrationsTestingEnabled()) {
      api(project(":nessie-deltalake"))
      api(project(":iceberg-views"))
      api(project(":nessie-spark-antlr-runtime"))
      api(project(":nessie-spark-extensions-grammar"))
      api(project(":nessie-gc-iceberg"))
      api(project(":nessie-gc-iceberg-files"))
      api(project(":nessie-gc-iceberg-mock"))
      api(project(":nessie-gc-tool"))

      val ideSyncActive =
        System.getProperty("idea.sync.active").toBoolean() ||
          System.getProperty("eclipse.product") != null ||
          gradle.startParameter.taskNames.any { it.startsWith("eclipse") }
      val sparkVersions = rootProject.extra["sparkVersions"].toString().split(",").map { it.trim() }
      val allScalaVersions = LinkedHashSet<String>()
      for (sparkVersion in sparkVersions) {
        val scalaVersions =
          rootProject.extra["sparkVersion-${sparkVersion}-scalaVersions"]
            .toString()
            .split(",")
            .map { it.trim() }
        for (scalaVersion in scalaVersions) {
          allScalaVersions.add(scalaVersion)
          api(project(":nessie-spark-extensions-${sparkVersion}_$scalaVersion"))
          if (ideSyncActive) {
            break
          }
        }
      }
      for (scalaVersion in allScalaVersions) {
        api(project(":nessie-spark-extensions-base_$scalaVersion"))
        if (ideSyncActive) {
          break
        }
      }
    }
  }
}

javaPlatform { allowDependencies() }
