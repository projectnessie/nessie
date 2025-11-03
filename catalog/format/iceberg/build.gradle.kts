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

description = "Nessie - Catalog - Iceberg table format"

val versionIceberg = libs.versions.iceberg.get()

sourceSets.register("avroSchema")

val avroSchemaImplementation by configurations.getting
val avroSchemaCompileOnly by configurations.getting
val avroSchemaAnnotationProcessor by configurations.getting
val avroSchemaRuntimeClasspath by configurations.getting

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

  avroSchemaImplementation(libs.avro)
  avroSchemaImplementation(libs.guava)
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

  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg")
}

val generatedAvroSchemas =
  layout.buildDirectory.asFile.map { it.resolve("generated/avroSchemas") }.get()

val generateAvroSchemas by
  tasks.registering(JavaExec::class) {
    dependsOn(tasks.named("avroSchemaClasses"))

    classpath(avroSchemaRuntimeClasspath, tasks.named("compileAvroSchemaJava"))

    mainClass.set("org.projectnessie.catalog.formats.iceberg.GenerateAvroSchemas")
    args("$generatedAvroSchemas/org/projectnessie/catalog/formats/iceberg")

    outputs.cacheIf { true }
    outputs.dir(generatedAvroSchemas)

    doFirst { delete(generatedAvroSchemas) }
  }

sourceSets.named("main") { resources { srcDir(generateAvroSchemas) } }
