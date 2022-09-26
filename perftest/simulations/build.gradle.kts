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

plugins {
  `maven-publish`
  signing
  alias(libs.plugins.gatling)
  `nessie-conventions`
  alias(libs.plugins.nessie.run)
}

extra["maven.name"] = "Nessie - Perf Test - Simulations"

dependencies {
  gatling(project(":nessie-model"))
  gatling(project(":nessie-client"))
  gatling(project(":nessie-perftest-gatling"))
  gatling(libs.gatling.charts.highcharts) {
    exclude("io.netty", "netty-tcnative-boringssl-static")
    exclude("commons-logging", "commons-logging")
  }
  gatling(libs.microprofile.openapi)

  gatling(platform(libs.jackson.bom))
  gatling(libs.jackson.annotations)

  nessieQuarkusServer(project(":nessie-quarkus", "quarkusRunner"))
}

nessieQuarkusApp {
  includeTasks(tasks.withType<GatlingRunTask>()) {
    jvmArgs(
      listOf(
        "-Dsim.users=10",
        "-Dnessie.uri=http://127.0.0.1:${extra["quarkus.http.test-port"]}/api/v1"
      )
    )
  }
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

gatling {
  gatlingVersion = libs.versions.gatling.get()
  // Null is OK (io.gatling.gradle.LogbackConfigTask checks for it)
  logLevel = System.getProperty("gatling.logLevel")
}
