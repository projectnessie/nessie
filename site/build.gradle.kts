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

val clean by tasks.registering(Delete::class) {
  delete("venv")
  delete(".cache")
  delete("build")
  delete("site")
}

/*
Not adding `siteServe`, because killing the build (ctrl-C) does not kill the subprocess.

val siteServe by tasks.registering(Exec::class) {
  group = "Site"
  description = "Serve the Nessie web site locally"
  commandLine("bin/serve.sh")
}
*/

val siteServe by tasks.registering {
  group = "Site"
  description = "Serve the Nessie web site locally"
  doFirst {
    logger.warn("Run ${project.layout.projectDirectory.file("bin/serve.sh")} from your shell")
  }
}

val siteBuild by tasks.registering(Exec::class) {
  group = "Site"
  description = "Build the Nessie web site locally"
  commandLine("bin/build.sh")
}
