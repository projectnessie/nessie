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

plugins { id("nessie-conventions-java17") }

description = "Nessie - Catalog - Iceberg table format"

val versionIceberg = libs.versions.iceberg.get()

sourceSets.register("avroSchema")

val avroSchemaImplementation = configurations.getByName("avroSchemaImplementation")
val avroSchemaCompileOnly = configurations.getByName("avroSchemaCompileOnly")
val avroSchemaAnnotationProcessor = configurations.getByName("avroSchemaAnnotationProcessor")
val avroSchemaRuntimeClasspath = configurations.getByName("avroSchemaRuntimeClasspath")

dependencies {
  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-model"))

  implementation(libs.guava)
  implementation(libs.avro)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  implementation(platform(libs.jackson3.bom))
  implementation("tools.jackson.core:jackson-databind")

  avroSchemaImplementation(libs.avro)
  avroSchemaImplementation(libs.guava)
  avroSchemaImplementation(libs.slf4j.nop)
  avroSchemaImplementation("org.apache.iceberg:iceberg-core:$versionIceberg")

  runtimeOnly(libs.zstd.jni)
  runtimeOnly(libs.snappy.java)

  implementation(libs.jakarta.annotation.api) // 'implementation' for smallrye-config
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi(project(":nessie-catalog-format-iceberg-fixturegen"))

  testFixturesImplementation(libs.guava)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesCompileOnly(platform(libs.jackson3.bom))
  testFixturesCompileOnly("tools.jackson.core:jackson-databind")

  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg")

  testCompileOnly(platform(libs.jackson3.bom))
  testCompileOnly("tools.jackson.core:jackson-databind")
}

testing {
  suites {
    register("testJackson3", JvmTestSuite::class.java) {
      useJUnitJupiter(libsRequiredVersion("junit"))

      dependencies {
        implementation.add(project())
        implementation.add(project(":nessie-catalog-model"))
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

val generateAvroSchemas =
  tasks.register<JavaExec>("generateAvroSchemas") {
    val generatedAvroSchemas = layout.buildDirectory.dir("generated/avroSchemas").map { it.asFile }

    dependsOn(tasks.named("avroSchemaClasses"))

    classpath(avroSchemaRuntimeClasspath, tasks.named("compileAvroSchemaJava"))

    mainClass.set("org.projectnessie.catalog.formats.iceberg.GenerateAvroSchemas")
    args("${generatedAvroSchemas.get()}/org/projectnessie/catalog/formats/iceberg")

    outputs.cacheIf { true }
    outputs.dir(generatedAvroSchemas)

    doFirst { generatedAvroSchemas.get().deleteRecursively() }
  }

sourceSets.named("main") { resources { srcDir(generateAvroSchemas) } }
