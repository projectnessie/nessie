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

plugins { id("nessie-conventions-java17") }

publishingHelper { mavenName = "Nessie - Catalog - Files API" }

dependencies {
  api(project(":nessie-storage-uri"))
  implementation(project(":nessie-catalog-model"))
  implementation(libs.guava)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  compileOnly(project(":nessie-doc-generator-annotations"))
  compileOnly(libs.smallrye.config.core)

  compileOnly(platform(libs.awssdk.bom))
  compileOnly("software.amazon.awssdk:iam-policy-builder")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-protobuf")

  compileOnly(platform(libs.jackson3.bom))
  compileOnly("tools.jackson.core:jackson-databind")
  runtimeOnly(platform(libs.jackson3.bom))
  runtimeOnly("tools.jackson.core:jackson-databind")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(platform(libs.awssdk.bom))
  testFixturesApi("software.amazon.awssdk:iam-policy-builder")
}

testing {
  suites {
    register("testJackson3", JvmTestSuite::class.java) {
      useJUnitJupiter(libsRequiredVersion("junit"))

      dependencies {
        implementation.add(project())
        implementation.add(platform(libs.jackson3.bom))
        implementation.add("tools.jackson.core:jackson-databind")
        compileOnly.add(platform(libs.jackson.bom))
        compileOnly.add("com.fasterxml.jackson.core:jackson-databind")
        implementation.add(platform(libs.junit.bom))
        implementation.add(libs.assertj.core)
      }

      targets {
        all {
          testTask.configure {
            usesService(
              gradle.sharedServices.registrations.named("testParallelismConstraint").get().service
            )
          }
          tasks.named("test").configure { dependsOn(testTask) }
        }
      }
    }
  }
}
