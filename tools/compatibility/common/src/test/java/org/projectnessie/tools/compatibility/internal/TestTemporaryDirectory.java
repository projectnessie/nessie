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
package org.projectnessie.tools.compatibility.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class TestTemporaryDirectory {
  @Test
  void tempDir() throws Throwable {
    TemporaryDirectory dir = new TemporaryDirectory();
    Path tempDir;
    try {
      TemporaryDirectory dir2 = new TemporaryDirectory();
      try {
        tempDir = dir.getPath();
        assertThat(tempDir).isDirectory().isNotEqualTo(dir2.getPath());

        assertThat(dir).isNotEqualTo(dir2);

        assertThat(ImmutableSet.of(dir, dir2)).contains(dir, dir2);

        assertThat(Stream.of(dir, dir2))
            .map(TemporaryDirectory::toString)
            .containsExactly(
                "TemporaryDirectory{path=" + dir.getPath() + '}',
                "TemporaryDirectory{path=" + dir2.getPath() + '}');

        Files.createFile(tempDir.resolve("some file"));
      } finally {
        dir2.close();
      }
    } finally {
      dir.close();
    }

    assertThat(tempDir).doesNotExist();
  }
}
