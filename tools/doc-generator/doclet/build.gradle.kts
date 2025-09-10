/*
 * Copyright (C) 2024 Dremio
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

plugins { id("nessie-conventions-java11") }

extra["maven.name"] =
  "Code to generate markdown documentation files for 'properties' and smallrye-config"

val genProjects by configurations.creating
val genSources by configurations.creating

val genProjectPaths =
  listOf(
    ":nessie-model",
    ":nessie-client",
    ":nessie-quarkus-common",
    ":nessie-quarkus-authn",
    ":nessie-quarkus-authz",
    ":nessie-versioned-storage-bigtable",
    ":nessie-versioned-storage-cassandra",
    ":nessie-versioned-storage-cassandra2",
    ":nessie-versioned-storage-common",
    ":nessie-versioned-storage-dynamodb",
    ":nessie-versioned-storage-dynamodb2",
    ":nessie-versioned-storage-inmemory",
    ":nessie-versioned-storage-jdbc",
    ":nessie-versioned-storage-jdbc2",
    ":nessie-versioned-storage-mongodb2",
    ":nessie-versioned-storage-rocksdb",
  )

dependencies {
  implementation(project(":nessie-doc-generator-annotations"))
  implementation(libs.smallrye.config.core)
  implementation(libs.commons.text)

  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  genProjects(project(":nessie-doc-generator-annotations"))
  genProjects(libs.smallrye.config.core)
}

tasks.named<Test>("test") {
  // The test needs the classpath for the necessary dependencies (annotations + smallrye-config).
  // Resolving the dependencies must happen during task execution (not configuration).
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      // So, in theory, all 'org.gradle.category' attributes should use the type
      // org.gradle.api.attributes.Category,
      // as Category.CATEGORY_ATTRIBUTE is defined. BUT! Some attributes have an attribute type ==
      // String.class!

      val categoryAttributeAsString = Attribute.of("org.gradle.category", String::class.java)

      val libraries =
        genProjects.incoming.artifacts
          .filter { a ->
            // dependencies:
            //  org.gradle.category=library
            val category =
              a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
                ?: a.variant.attributes.getAttribute(categoryAttributeAsString)
            category != null && category.toString() == Category.LIBRARY
          }
          .map { a -> a.file }

      listOf("-Dtesting.libraries=" + libraries.joinToString(":"))
    }
  )
}
