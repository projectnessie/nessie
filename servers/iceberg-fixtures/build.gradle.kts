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

extra["maven.name"] = "Nessie - Iceberg Metadata test data"

plugins {
  `java-library`
  jacoco
  `maven-publish`
  `nessie-conventions`
}

dependencies {
  compileOnly(platform(rootProject))
  annotationProcessor(platform(rootProject))

  compileOnly(platform("io.quarkus:quarkus-bom"))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  compileOnly(project(":iceberg-views"))
  compileOnly("org.apache.iceberg:iceberg-core")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.google.code.findbugs:jsr305")

  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  testCompileOnly(platform(rootProject))
  testAnnotationProcessor(platform(rootProject))
  testImplementation(project(":iceberg-views"))
  testImplementation("org.apache.iceberg:iceberg-core")
  testImplementation(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testImplementation("com.fasterxml.jackson.core:jackson-databind")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-engine")
  testImplementation("org.junit.jupiter:junit-jupiter-params")

  testCompileOnly(platform("io.quarkus:quarkus-bom"))
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
}

val generatedIcebergMetadataDir = project.buildDir.resolve("generated/iceberg/main")

val generateIcebergMetadataFiles by
  tasks.registering(DefaultTask::class) {
    val javaCompile = tasks.getByName<JavaCompile>("compileJava")
    dependsOn(javaCompile)
    inputs.dir(javaCompile.destinationDirectory)
    inputs.files(configurations.testRuntimeClasspath)
    outputs.dir(generatedIcebergMetadataDir)
    outputs.cacheIf { true }

    doFirst {
      javaexec {
        classpath =
          configurations.testRuntimeClasspath.get().plus(files(javaCompile.destinationDirectory))
        mainClass.set("org.projectnessie.iceberg.metadata.GenerateIcebergFiles")
        args(
          generatedIcebergMetadataDir
            .resolve("org/projectnessie/test-data/iceberg-metadata")
            .absolutePath
        )
      }
    }
  }

sourceSets.named("main") {
  resources.srcDir(generatedIcebergMetadataDir)
  compiledBy(generateIcebergMetadataFiles)
}
