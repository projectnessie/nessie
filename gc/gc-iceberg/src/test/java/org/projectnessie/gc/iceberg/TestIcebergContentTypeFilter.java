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
package org.projectnessie.gc.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;

public class TestIcebergContentTypeFilter {

  static Stream<Content.Type> nonIcebergTable() {
    return Arrays.stream(ContentTypes.all())
        .filter(t -> !ICEBERG_TABLE.equals(t) && !ICEBERG_VIEW.equals(t));
  }

  @ParameterizedTest
  @MethodSource("nonIcebergTable")
  public void nonIcebergTable(Content.Type contentType) {
    assertThat(IcebergContentTypeFilter.INSTANCE.test(contentType)).isFalse();
  }

  @Test
  public void icebergTable() {
    assertThat(IcebergContentTypeFilter.INSTANCE.test(ICEBERG_TABLE)).isTrue();
    assertThat(IcebergContentTypeFilter.INSTANCE.test(ICEBERG_VIEW)).isTrue();
  }
}
