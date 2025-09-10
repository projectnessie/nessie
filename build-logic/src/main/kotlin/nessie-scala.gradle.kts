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

tasks.withType<ScalaCompile>().configureEach {
  scalaCompileOptions.keepAliveMode = KeepAliveMode.DAEMON
  scalaCompileOptions.encoding = "UTF-8"
}

val scaladoc = tasks.named<ScalaDoc>("scaladoc")

if (extensions.findByName("jandex") != null) {
  scaladoc.configure { dependsOn(tasks.named("jandex")) }
}

val scaladocJar = tasks.register<Jar>("scaladocJar")

scaladocJar.configure {
  dependsOn(scaladoc)
  val baseJar = tasks.getByName<Jar>("jar")
  from(scaladoc.get().destinationDir)
  destinationDirectory = baseJar.destinationDirectory
  archiveClassifier = "scaladoc"
}

tasks.named("assemble").configure { dependsOn(scaladocJar) }

configure<PublishingExtension> {
  publications {
    withType(MavenPublication::class.java) {
      if (name == "maven") {
        artifact(scaladocJar)
      }
    }
  }
}
