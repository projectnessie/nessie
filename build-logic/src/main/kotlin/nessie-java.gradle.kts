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

tasks.withType<Jar>().configureEach {
  manifest {
    attributes["Implementation-Title"] = "Nessie ${project.name}"
    attributes["Implementation-Version"] = project.version
    attributes["Implementation-Vendor"] = "Dremio"
  }
  duplicatesStrategy = DuplicatesStrategy.WARN
}

tasks.withType<JavaCompile>().configureEach {
  options.encoding = "UTF-8"
  options.compilerArgs.add("-parameters")

  // Required to enable incremental compilation w/ immutables, see
  // https://github.com/immutables/immutables/pull/858 and
  // https://github.com/immutables/immutables/issues/804#issuecomment-487366544
  options.compilerArgs.add("-Aimmutables.gradle.incremental")

  // Warn on deprecated-for-removal
  options.compilerArgs.add("-Xlint:removal")
  // Warn on preview features being used
  options.compilerArgs.add("-Xlint:preview")

  doFirst {
    if (javaCompiler.get().metadata.languageVersion.asInt() >= 21) {
      // Warns when a single output class file is written more than once, e.g.
      // with a case-insensitive file system. See JDK-8296656.
      options.compilerArgs.add("-Xlint:output-file-clash")
      // Warns on ambiguous overloads, see JDK-8026369
      options.compilerArgs.add("-Xlint:overloads")
    }
  }
}

tasks.withType<Javadoc>().configureEach {
  val opt = options as CoreJavadocOptions
  // don't spam log w/ "warning: no @param/@return"
  opt.addStringOption("Xdoclint:-reference", "-quiet")
}

plugins.withType<JavaPlugin>().configureEach {
  configure<JavaPluginExtension> {
    withJavadocJar()
    withSourcesJar()
  }
}
