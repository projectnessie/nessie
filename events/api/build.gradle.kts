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

plugins { id("nessie-conventions-java17") }

publishingHelper { mavenName = "Nessie - Events - API" }

dependencies {
  api(project(":nessie-model"))

  // Immutables
  implementation(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  // Jackson
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly(platform(libs.jackson3.bom))
  compileOnly("tools.jackson.core:jackson-databind")
  runtimeOnly(platform(libs.jackson3.bom))
  runtimeOnly("tools.jackson.core:jackson-databind")

  compileOnly(libs.microprofile.openapi)

  // Testing
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)

  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")

  testCompileOnly(libs.microprofile.openapi)
}

testing {
  suites {
    register("testJackson3", JvmTestSuite::class.java) {
      useJUnitJupiter(libsRequiredVersion("junit"))

      dependencies {
        implementation.add(project())
        implementation.add(platform(libs.jackson3.bom))
        implementation.add("tools.jackson.core:jackson-databind")
        implementation.add(platform(libs.junit.bom))
        implementation.add(libs.assertj.core)
        compileOnly(libs.microprofile.openapi)
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

sourceSets.named("testJackson3") { resources { srcDir("src/test/resources") } }
