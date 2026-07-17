/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.gc.files;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public class TestFileReference {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void relativePath() {
    StorageUri base = StorageUri.of("s3://bucket/warehouse/table/");
    FileReference ref = FileReference.of(StorageUri.of("data/file-1.parquet"), base, -1L);

    soft.assertThat(ref.absolutePath())
        .isEqualTo(StorageUri.of("s3://bucket/warehouse/table/data/file-1.parquet"));
  }

  @Test
  public void absolutePathOutsideBase() {
    // Files outside the content's base location, see
    // https://github.com/projectnessie/nessie/issues/10817
    StorageUri base = StorageUri.of("s3://bucket/warehouse/table/");
    StorageUri file = StorageUri.of("s3://other-bucket/elsewhere/file-1.parquet");
    FileReference ref = FileReference.of(file, base, -1L);

    soft.assertThat(ref.path()).isEqualTo(file);
    soft.assertThat(ref.absolutePath()).isEqualTo(file);
  }

  @Test
  public void relativeBaseRejected() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                FileReference.of(
                    StorageUri.of("data/file-1.parquet"), StorageUri.of("warehouse/table/"), -1L))
        .withMessageStartingWith("Base location must be absolute");
  }
}
