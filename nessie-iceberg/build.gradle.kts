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
  signing
  `maven-publish`
  id("org.projectnessie.buildsupport.ide-integration")
  `nessie-conventions`
}

// Pull the extra properties from the "main" Nessie build. This ensures that the same versions
// are being used and we don't need to tweak the dependabot config.
val extraPropertyPattern = java.util.regex.Pattern.compile("va[lr] (version[A-Z][A-Za-z0-9_]+) = \"([0-9a-zA-Z-.]+)\"")
file("../build.gradle.kts").readLines().forEach { line ->
  val lineMatch = extraPropertyPattern.matcher(line)
  if (lineMatch.matches()) {
    extra[lineMatch.group(1)] = lineMatch.group(2)
  }
}

for (e in loadProperties(file("../clients/spark-scala.properties"))) {
  extra[e.key.toString()] = e.value
}

publishingHelper {
  nessieRepoName.set("nessie")
  inceptionYear.set("2020")
}
