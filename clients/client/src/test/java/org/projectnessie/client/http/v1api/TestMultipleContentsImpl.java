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
package org.projectnessie.client.http.v1api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.MultipleContents;
import org.projectnessie.error.NessieContentsNotFoundException;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;

class TestMultipleContentsImpl {

  private static final ContentsKey key1 = ContentsKey.of("key1");
  private static final ContentsKey key2 = ContentsKey.of("key2");
  private static final ContentsKey key3 = ContentsKey.of("key3");
  private static final IcebergTable table1 = IcebergTable.of("meta1", "id1");
  private static final IcebergTable table2 = IcebergTable.of("meta2", "id2");
  private static final ContentsWithKey c1 = ContentsWithKey.of(key1, table1);
  private static final ContentsWithKey c2 = ContentsWithKey.of(key2, table2);

  @Test
  void testMapApi() {
    MultipleContents contents = MultipleContentsImpl.of(ImmutableList.of(c1, c2));
    assertThat(contents).hasSize(2);
    assertThat(contents).doesNotContainKey(key3);
    assertThat(contents)
        .extractingByKey(key1)
        .satisfies(c -> assertThat(c.unwrap(IcebergTable.class)).hasValue(table1));
    assertThat(contents).containsKey(key2);
  }

  @Test
  void testSingle() throws NessieContentsNotFoundException {
    MultipleContents contents = MultipleContentsImpl.of(ImmutableList.of(c1, c2));
    assertThat(contents.single(key1).getId()).isEqualTo(table1.getId());
    assertThatThrownBy(() -> contents.single(key3))
        .isInstanceOf(NessieContentsNotFoundException.class)
        .hasMessageContaining(key3.toString());
  }

  @Test
  void testUnwrap() {
    MultipleContents contents = MultipleContentsImpl.of(ImmutableList.of(c1, c2));
    assertThat(contents.unwrap(key1, IcebergTable.class)).hasValue(table1);
    assertThat(contents.unwrap(key1, String.class)).isEmpty();
    assertThat(contents.unwrap(key3, IcebergTable.class)).isEmpty();
  }

  @Test
  void testUnwrapSingle() throws NessieContentsNotFoundException {
    MultipleContents contents = MultipleContentsImpl.of(ImmutableList.of(c1, c2));
    assertThat(contents.unwrapSingle(key1, IcebergTable.class)).isEqualTo(table1);
    assertThat(contents.<IcebergTable>unwrapSingle(key1)).isEqualTo(table1);

    assertThatThrownBy(() -> contents.unwrapSingle(key1, TestMultipleContentsImpl.class))
        .isInstanceOf(NessieContentsNotFoundException.class)
        .hasMessageContaining(TestMultipleContentsImpl.class.getSimpleName());

    assertThatThrownBy(() -> contents.unwrapSingle(key3, IcebergTable.class))
        .isInstanceOf(NessieContentsNotFoundException.class)
        .hasMessageContaining(key3.toString());
  }
}
