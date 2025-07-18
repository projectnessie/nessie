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

import io.gatling.gradle.GatlingRunTask
import java.util.Map.entry as mapEntry

plugins {
  id("nessie-conventions-scala")
  alias(libs.plugins.gatling)
  alias(libs.plugins.nessie.run)
}

publishingHelper { mavenName = "Nessie - Perf Test - Simulations" }

dependencies {
  if (System.getProperty("idea.sync.active").toBoolean()) {
    // IJ complains about Scala-library not present (it's there for the 'gatling' source set).
    compileOnly("org.scala-lang:scala-library:${scalaDependencyVersion("2.13")}")
  }
  gatling(project(":nessie-model"))
  gatling(project(":nessie-client"))
  gatling(project(":nessie-perftest-gatling"))
  gatling(libs.gatling.charts.highcharts) {
    exclude("io.netty", "netty-tcnative-boringssl-static")
    exclude("commons-logging", "commons-logging")
  }
  gatling(libs.microprofile.openapi)

  gatling(platform(libs.jackson.bom))
  gatling("com.fasterxml.jackson.core:jackson-annotations")

  nessieQuarkusServer(project(":nessie-quarkus", "quarkusRunner"))
}

nessieQuarkusApp {
  if (!System.getProperties().containsKey("nessie.uri")) {
    includeTasks(tasks.withType<GatlingRunTask>()) {
      jvmArgs = listOf("-Dsim.users=10", "-Dnessie.uri=${extra["quarkus.http.test-url"]}/api/v2")
    }
    environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
    jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
    systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
    System.getProperties()
      .map { e -> mapEntry(e.key.toString(), e.value.toString()) }
      .filter { e -> e.key.startsWith("nessie.") || e.key.startsWith("quarkus.") }
      .forEach { e -> systemProperties.put(e.key, e.value) }
  }
}

tasks.withType(GatlingRunTask::class.java).configureEach {
  inputs.files(configurations.getByName("gatlingRuntimeClasspath"))
}

gatling {
  gatlingVersion = libs.versions.gatling.get()

  jvmArgs =
    System.getProperties()
      .map { e -> mapEntry(e.key.toString(), e.value.toString()) }
      .filter { e ->
        e.key.startsWith("nessie.") || e.key.startsWith("gatling.") || e.key.startsWith("sim.")
      }
      .map { e ->
        if (e.key.startsWith("nessie.") || e.key.startsWith("sim.")) {
          "-D${e.key}=${e.value}"
        } else if (e.key.startsWith("gatling.jvmArg")) {
          e.value
        } else if (e.key.startsWith("gatling.")) {
          "-D${e.key.substring("gatling.".length)}=${e.value}"
        } else {
          throw IllegalStateException("Unexpected: ${e.key}")
        }
      }
}
