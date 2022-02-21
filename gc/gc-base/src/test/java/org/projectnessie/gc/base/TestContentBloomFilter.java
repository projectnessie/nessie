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
package org.projectnessie.gc.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.Content.Type.DELTA_LAKE_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;

public class TestContentBloomFilter {

  @Test
  void testBasic() {
    ContentBloomFilter filter = new ContentBloomFilter(10, 0.03d);
    List<Content> contents = generateContents(0, ICEBERG_TABLE);
    contents.addAll(generateContents(0, ICEBERG_VIEW));
    contents.forEach(filter::put);
    // has 10 live iceberg table contents (0-9) snapshot id
    // and 10 live iceberg view contents (0-9) version id

    // bloom filter must return true for existing contents
    assertThat(filter.mightContain(contents.get(5))).isTrue();
    assertThat(filter.mightContain(contents.get(12))).isTrue();

    // cannot test non-existing contents as bloom may return true or false because of false
    // positive.
    // Testcase will become flaky if we expect fixed value always.
  }

  @Test
  void testMerge() {
    ContentBloomFilter filter1 = new ContentBloomFilter(10, 0.03d);
    List<Content> contents1 = generateContents(0, ICEBERG_TABLE);
    contents1.addAll(generateContents(0, ICEBERG_VIEW));
    contents1.forEach(filter1::put);

    // create another filter with overlapping contents
    ContentBloomFilter filter2 = new ContentBloomFilter(10, 0.03d);
    List<Content> contents2 = generateContents(5, ICEBERG_TABLE);
    contents2.addAll(generateContents(0, ICEBERG_VIEW));
    contents2.forEach(filter2::put);

    filter1.merge(filter2);

    // merged filter should contain contents from both the filters.
    assertThat(filter1.mightContain(contents1.get(4))).isTrue();
    assertThat(filter1.mightContain(contents2.get(2))).isTrue();
    assertThat(filter1.mightContain(contents2.get(8))).isTrue();
  }

  @Test
  void testDeltaLakeContent() {
    ContentBloomFilter filter = new ContentBloomFilter(10, 0.03d);
    List<Content> contents = generateContents(0, DELTA_LAKE_TABLE);

    assertThatThrownBy(() -> contents.forEach(filter::put))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Unsupported type DELTA_LAKE_TABLE");
  }

  private List<Content> generateContents(int index, Content.Type contentType) {
    List<Content> contents = new ArrayList<>();
    for (int i = index; i < 10 + index; i++) {
      switch (contentType) {
        case ICEBERG_TABLE:
          contents.add(IcebergTable.of("temp" + i, i, 42, 42, 42));
          break;
        case ICEBERG_VIEW:
          contents.add(IcebergView.of("temp" + i, i, 42, "dialect" + i, "sql" + i));
          break;
        case DELTA_LAKE_TABLE:
          contents.add(
              ImmutableDeltaLakeTable.builder()
                  .id(String.valueOf(i))
                  .addAllCheckpointLocationHistory(Collections.emptyList())
                  .addAllMetadataLocationHistory(Collections.emptyList())
                  .build());
          break;
        default:
          throw new RuntimeException("Unsupported type: " + contentType);
      }
    }
    return contents;
  }
}
