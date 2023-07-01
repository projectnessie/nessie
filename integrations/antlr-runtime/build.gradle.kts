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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("nessie-conventions-client")
  id("nessie-shadow-jar")
}

extra["maven.name"] = "Nessie - Antlr Runtime"

dependencies { implementation(libs.antlr.antlr4.runtime) }

tasks.named<ShadowJar>("shadowJar").configure {
  dependencies { include(dependency("org.antlr:antlr4-runtime")) }
  relocate("org.antlr.v4.runtime", "org.projectnessie.shaded.org.antlr.v4.runtime")
}
