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

apply<PublishingHelperPlugin>()

apply<NessieIdePlugin>()

apply<NessieSpotlessPlugin>()

apply<NessieJandexPlugin>()

apply<NessieJavaPlugin>()

apply<NessieScalaPlugin>()

if (projectDir.resolve("src").exists()) {
  apply<NessieCheckstylePlugin>()

  apply<NessieErrorpronePlugin>()

  apply<NessieTestingPlugin>()

  apply<NessieCodeCoveragePlugin>()
}

tasks.register("compileAll") {
  group = "build"
  description = "Runs all compilation and jar tasks"
  dependsOn(tasks.withType<AbstractCompile>(), tasks.withType<ProcessResources>())
}
