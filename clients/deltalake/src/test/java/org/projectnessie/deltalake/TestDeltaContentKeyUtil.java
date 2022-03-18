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
package org.projectnessie.deltalake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

public class TestDeltaContentKeyUtil {

  @Test
  public void nullAndEmptyAndInvalid() {
    assertThatThrownBy(() -> DeltaContentKeyUtil.fromFilePathString(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(DeltaContentKeyUtil.INVALID_PATH_MSG(), (String) null));

    assertThatThrownBy(() -> DeltaContentKeyUtil.fromFilePathString(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(DeltaContentKeyUtil.INVALID_PATH_MSG(), ""));

    assertThatThrownBy(() -> DeltaContentKeyUtil.fromFilePathString("//"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(String.format(DeltaContentKeyUtil.INVALID_PATH_MSG(), "//"));

    // these path strings are only valid via the Hadoop path
    assertThatThrownBy(() -> DeltaContentKeyUtil.fromFilePathString("file:///tmp/a/b/c/"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(String.format(DeltaContentKeyUtil.INVALID_PATH_MSG(), "file:///tmp/a/b/c/"));

    assertThatThrownBy(() -> DeltaContentKeyUtil.fromHadoopPath(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(DeltaContentKeyUtil.INVALID_HADOOP_PATH_MSG(), (String) null));
  }

  @Test
  public void contentKeyFromPathString() {
    assertThat(DeltaContentKeyUtil.fromFilePathString("/tmp/a/b/c/"))
        .isEqualTo(ContentKey.of("tmp", "a", "b", "c"));

    assertThat(DeltaContentKeyUtil.fromFilePathString("/a/b//")).isEqualTo(ContentKey.of("a", "b"));
    assertThat(DeltaContentKeyUtil.fromFilePathString("/a/b//////"))
        .isEqualTo(ContentKey.of("a", "b"));
    assertThat(DeltaContentKeyUtil.fromFilePathString("//a")).isEqualTo(ContentKey.of("a"));
    assertThat(DeltaContentKeyUtil.fromFilePathString("//////a")).isEqualTo(ContentKey.of("a"));
  }

  @Test
  public void contentKeyFromHadoopPath() {
    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("tmp"))).isEqualTo(ContentKey.of("tmp"));
    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("tmp/a/b/c")))
        .isEqualTo(ContentKey.of("tmp", "a", "b", "c"));

    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("/a/b//")))
        .isEqualTo(ContentKey.of("a", "b"));
    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("/a/b//////")))
        .isEqualTo(ContentKey.of("a", "b"));

    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("///a"))).isEqualTo(ContentKey.of("a"));
    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("//////a")))
        .isEqualTo(ContentKey.of("a"));

    assertThat(DeltaContentKeyUtil.fromHadoopPath(new Path("file:///tmp/a/b/c/")))
        .isEqualTo(ContentKey.of("tmp", "a", "b", "c"));
  }
}
