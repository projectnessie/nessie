/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.quarkus.runner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestJavaVM {
  @Test
  void checkJavaVersionStrings() {
    assertThat(JavaVM.majorVersionFromString("11")).isEqualTo(11);
    assertThat(JavaVM.majorVersionFromString("17.0.1")).isEqualTo(17);
    assertThat(JavaVM.majorVersionFromString("1.8.0-foo+bar")).isEqualTo(8);
  }

  @Test
  void checkResolveEnvJdkHomeLinux() {
    Map<String, String> env = new HashMap<>();
    env.put("JDK11_HOME", "/mycomputer/java11");
    env.put("JAVA17_HOME", "/mycomputer/java17");

    Map<String, String> sysProps = new HashMap<>();
    sysProps.put("jdk9.home", "/mycomputer/java9");
    sysProps.put("java10.home", "/mycomputer/java10");
    sysProps.put("os.name", "Linux");

    assertThat(JavaVM.locateJavaHome(8, env::get, sysProps::get, i -> "/hello/there"))
        .isNull();
    assertThat(JavaVM.locateJavaHome(9, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java9");
    assertThat(JavaVM.locateJavaHome(10, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java10");
    assertThat(JavaVM.locateJavaHome(11, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java11");
    assertThat(JavaVM.locateJavaHome(14, env::get, sysProps::get, i -> "/hello/there"))
        .isNull();
    assertThat(JavaVM.locateJavaHome(17, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java17");
  }

  @Test
  void checkResolveEnvJdkHomeMacOS() {
    Map<String, String> env = new HashMap<>();
    env.put("JDK11_HOME", "/mycomputer/java11");
    env.put("JAVA17_HOME", "/mycomputer/java17");

    Map<String, String> sysProps = new HashMap<>();
    sysProps.put("jdk9.home", "/mycomputer/java9");
    sysProps.put("java10.home", "/mycomputer/java10");
    sysProps.put("os.name", "Darwin");

    assertThat(JavaVM.locateJavaHome(8, env::get, sysProps::get, i -> i == 8 ? "/from_java_home/v8" : null))
        .isEqualTo("/from_java_home/v8");
    assertThat(JavaVM.locateJavaHome(9, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java9");
    assertThat(JavaVM.locateJavaHome(10, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java10");
    assertThat(JavaVM.locateJavaHome(11, env::get, sysProps::get, i -> "/hello/there"))
        .isEqualTo("/mycomputer/java11");
    assertThat(JavaVM.locateJavaHome(14, env::get, sysProps::get, i -> i >= 12 ? "/from_java_home/v16" : null))
        .isEqualTo("/from_java_home/v16");
    assertThat(JavaVM.locateJavaHome(17, env::get, sysProps::get, i -> i >= 12 ? "/from_java_home/v8" : null))
        .isEqualTo("/mycomputer/java17");
  }

  @Test
  void checkJreResolve(@TempDir Path jdkDir) throws Exception {
    Path jdkBinDir = jdkDir.resolve("bin");
    Path jdkJavaFile = jdkBinDir.resolve(JavaVM.executableName("java"));
    Path jreDir = jdkDir.resolve("jre");
    Path jreBinDir = jreDir.resolve("bin");
    Path jreJavaFile = jreBinDir.resolve(JavaVM.executableName("java"));

    Files.createDirectories(jdkBinDir);
    Files.createDirectories(jreBinDir);
    Files.createFile(jreJavaFile, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));

    Map<String, String> env = new HashMap<>();
    env.put("JDK8_HOME", jreDir.toString());

    Map<String, String> sysProps = new HashMap<>();
    sysProps.put("os.name", "Linux");

    assertThat(JavaVM.locateJavaHome(8, env::get, sysProps::get, x -> null))
        .isEqualTo(jreDir.toString());
    assertThat(JavaVM.fixJavaHome(jreDir))
        .isEqualTo(jreDir);
    assertThat(JavaVM.forJavaHome(jreDir).getJavaExecutable())
        .isEqualTo(jreJavaFile);
    assertThat(JavaVM.forJavaHome(jreDir).getJavaHome())
        .isEqualTo(jreDir);
    assertThat(JavaVM.forJavaHome(jreDir.toString()).getJavaHome())
        .isEqualTo(jreDir);

    Files.createFile(jdkJavaFile, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---")));

    assertThat(JavaVM.fixJavaHome(jreDir))
        .isEqualTo(jdkDir);
    assertThat(JavaVM.forJavaHome(jreDir).getJavaExecutable())
        .isEqualTo(jdkJavaFile);
    assertThat(JavaVM.forJavaHome(jreDir).getJavaHome())
        .isEqualTo(jdkDir);
    assertThat(JavaVM.forJavaHome(jreDir.toString()).getJavaHome())
        .isEqualTo(jdkDir);
  }
}
