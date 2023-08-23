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
  antlr
  id("nessie-conventions-client")
}

dependencies {
  antlr(libs.antlr.antlr4)

  api(project(":nessie-spark-antlr-runtime", "shadow"))
}

sourceSets { main { antlr { setSrcDirs(listOf(project.projectDir.resolve("src/main/antlr4"))) } } }

// Gradle's implementation of the antlr plugin creates a configuration called "antlr" and lets
// the "api" configuration extend "antlr", which leaks the antlr tool and runtime plus dependencies
// to all users of this project. So do not let "api" extend from "antlr".
configurations.api.get().setExtendsFrom(listOf())

tasks.named<AntlrTask>("generateGrammarSource").configure {
  arguments.add("-visitor")
  doLast(
    ReplaceInFiles(
      fileTree(project.layout.buildDirectory.asFile.map { it.resolve("generated-src/antlr/main") })
        .matching { include("**/*.java") },
      mapOf(
        "import org.antlr.v4.runtime." to "import org.projectnessie.shaded.org.antlr.v4.runtime.",
        "// PACKAGE_PLACEHOLDER" to "package org.apache.spark.sql.catalyst.parser.extensions;"
      )
    )
  )
}

tasks.withType<Checkstyle>().configureEach {
  // Cannot exclude build/ as a "general coguration", because the Checstyle task creates an
  // ant script behind the scenes, and that only supports "string" pattern matching using.
  // The base directores are the source directories, so all patterns match against paths
  // relative to a source-directory, not against full path names, not even relative to the current
  // project.
  exclude("org/apache/spark/sql/catalyst/parser/extensions/*")
}
