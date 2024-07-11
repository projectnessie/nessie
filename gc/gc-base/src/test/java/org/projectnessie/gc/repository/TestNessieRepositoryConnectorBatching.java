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
package org.projectnessie.gc.repository;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.gc.repository.NessieRepositoryConnector.CONTENT_BATCH_SIZE;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieRepositoryConnectorBatching {
  public static final ImmutableSet<Content.Type> ICEBERG_CONTENT_TYPES =
      ImmutableSet.of(ICEBERG_TABLE, ICEBERG_VIEW);

  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void batchContentsFetchingNoKeysNoValues() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream()).thenReturn(Stream.empty());

    when(api.getEntries()).thenReturn(getEntries);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, ICEBERG_CONTENT_TYPES)).isEmpty();
    }

    verify(api, times(1)).getEntries();
    verify(api, never()).getContent();
  }

  @Test
  public void batchContentsFetchingSomeKeysNoValues() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    IntFunction<ContentKey> key = i -> ContentKey.of("key-" + i);
    IntFunction<EntriesResponse.Entry> keyEntry =
        i -> EntriesResponse.Entry.entry(key.apply(i), ICEBERG_TABLE, UUID.randomUUID().toString());

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream()).thenReturn(IntStream.rangeClosed(1, 3).mapToObj(keyEntry));

    GetContentBuilder getContent = mock(GetContentBuilder.class);
    when(getContent.reference(ref)).thenReturn(getContent);
    when(getContent.keys(any())).thenReturn(getContent);
    when(getContent.get()).thenReturn(emptyMap());

    when(api.getEntries()).thenReturn(getEntries);
    when(api.getContent()).thenReturn(getContent);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, singleton(ICEBERG_TABLE))).isEmpty();
    }

    verify(api, times(1)).getEntries();
    verify(api, times(1)).getContent();
  }

  @Test
  public void batchContentsFetchingSomeKeysAndValues() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    IntFunction<ContentKey> key = i -> ContentKey.of("key-" + i);
    IntFunction<Content> content = i -> IcebergTable.of("meta-" + i, 42, 43, 44, 45, "cid-" + i);
    IntFunction<EntriesResponse.Entry> keyEntry =
        i -> EntriesResponse.Entry.entry(key.apply(i), ICEBERG_TABLE, UUID.randomUUID().toString());

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream()).thenReturn(IntStream.rangeClosed(1, 3).mapToObj(keyEntry));

    Map<ContentKey, Content> expected =
        IntStream.rangeClosed(1, 3).boxed().collect(Collectors.toMap(key::apply, content::apply));

    GetContentBuilder getContent = mock(GetContentBuilder.class);
    when(getContent.reference(ref)).thenReturn(getContent);
    when(getContent.keys(any())).thenReturn(getContent);
    when(getContent.get()).thenReturn(expected);

    when(api.getEntries()).thenReturn(getEntries);
    when(api.getContent()).thenReturn(getContent);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, ICEBERG_CONTENT_TYPES))
          .containsExactlyInAnyOrderElementsOf(expected.entrySet());
    }

    verify(api, times(1)).getEntries();
    verify(api, times(1)).getContent();
  }

  @Test
  public void batchContentsFetchingBatchKeys() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    IntFunction<ContentKey> key = i -> ContentKey.of("key-" + i);
    IntFunction<Content> content = i -> IcebergTable.of("meta-" + i, 42, 43, 44, 45, "cid-" + i);
    IntFunction<EntriesResponse.Entry> keyEntry =
        i -> EntriesResponse.Entry.entry(key.apply(i), ICEBERG_TABLE, UUID.randomUUID().toString());

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream())
        .thenReturn(IntStream.rangeClosed(1, CONTENT_BATCH_SIZE).mapToObj(keyEntry));

    Map<ContentKey, Content> expected =
        IntStream.rangeClosed(1, CONTENT_BATCH_SIZE)
            .boxed()
            .collect(Collectors.toMap(key::apply, content::apply));

    GetContentBuilder getContent = mock(GetContentBuilder.class);
    when(getContent.reference(ref)).thenReturn(getContent);
    when(getContent.keys(any())).thenReturn(getContent);
    when(getContent.get()).thenReturn(expected);

    when(api.getEntries()).thenReturn(getEntries);
    when(api.getContent()).thenReturn(getContent);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, ICEBERG_CONTENT_TYPES))
          .containsExactlyInAnyOrderElementsOf(expected.entrySet());
    }

    verify(api, times(1)).getEntries();
    verify(api, times(1)).getContent();
  }

  @Test
  public void batchContentsFetchingBatchKeysPlus1() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    IntFunction<ContentKey> key = i -> ContentKey.of("key-" + i);
    IntFunction<Content> content = i -> IcebergTable.of("meta-" + i, 42, 43, 44, 45, "cid-" + i);
    IntFunction<EntriesResponse.Entry> keyEntry =
        i -> EntriesResponse.Entry.entry(key.apply(i), ICEBERG_TABLE, UUID.randomUUID().toString());

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream())
        .thenReturn(IntStream.rangeClosed(1, CONTENT_BATCH_SIZE + 1).mapToObj(keyEntry));

    Map<ContentKey, Content> expected =
        IntStream.rangeClosed(1, CONTENT_BATCH_SIZE + 1)
            .boxed()
            .collect(Collectors.toMap(key::apply, content::apply));
    Map<ContentKey, Content> contents1 =
        IntStream.rangeClosed(1, CONTENT_BATCH_SIZE)
            .boxed()
            .collect(Collectors.toMap(key::apply, content::apply));
    Map<ContentKey, Content> contents2 =
        IntStream.rangeClosed(CONTENT_BATCH_SIZE + 1, CONTENT_BATCH_SIZE + 1)
            .boxed()
            .collect(Collectors.toMap(key::apply, content::apply));

    GetContentBuilder getContent1 = mock(GetContentBuilder.class);
    when(getContent1.reference(ref)).thenReturn(getContent1);
    when(getContent1.keys(any())).thenReturn(getContent1);
    when(getContent1.get()).thenReturn(contents1);

    GetContentBuilder getContent2 = mock(GetContentBuilder.class);
    when(getContent2.reference(ref)).thenReturn(getContent2);
    when(getContent2.keys(any())).thenReturn(getContent2);
    when(getContent2.get()).thenReturn(contents2);

    when(api.getEntries()).thenReturn(getEntries);
    when(api.getContent()).thenReturn(getContent1).thenReturn(getContent2);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, ICEBERG_CONTENT_TYPES))
          .containsExactlyInAnyOrderElementsOf(expected.entrySet());
    }

    verify(api, times(1)).getEntries();
    verify(api, times(2)).getContent();
  }

  @Test
  public void batchContentsFetchingTenBatches() throws Exception {
    NessieApiV1 api = mock(NessieApiV1.class);

    Detached ref = Detached.of("00000000");

    IntFunction<ContentKey> key = i -> ContentKey.of("key-" + i);
    IntFunction<Content> content = i -> IcebergTable.of("meta-" + i, 42, 43, 44, 45, "cid-" + i);
    IntFunction<EntriesResponse.Entry> keyEntry =
        i -> EntriesResponse.Entry.entry(key.apply(i), ICEBERG_TABLE, UUID.randomUUID().toString());

    GetEntriesBuilder getEntries = mock(GetEntriesBuilder.class);
    when(getEntries.reference(ref)).thenReturn(getEntries);
    when(getEntries.stream())
        .thenReturn(IntStream.rangeClosed(1, CONTENT_BATCH_SIZE * 10).mapToObj(keyEntry));

    Map<ContentKey, Content> expected =
        IntStream.rangeClosed(1, CONTENT_BATCH_SIZE * 10)
            .boxed()
            .collect(Collectors.toMap(key::apply, content::apply));

    GetContentBuilder firstGetContent = null;
    GetContentBuilder[] nextGetContent = new GetContentBuilder[9];

    for (int page = 0; page < 10; page++) {
      Map<ContentKey, Content> pageContents =
          IntStream.rangeClosed(page * CONTENT_BATCH_SIZE + 1, (page + 1) * CONTENT_BATCH_SIZE)
              .boxed()
              .collect(Collectors.toMap(key::apply, content::apply));

      GetContentBuilder getContent = mock(GetContentBuilder.class);
      when(getContent.reference(ref)).thenReturn(getContent);
      when(getContent.keys(any())).thenReturn(getContent);
      when(getContent.get()).thenReturn(pageContents);

      if (page == 0) {
        firstGetContent = getContent;
      } else {
        nextGetContent[page - 1] = getContent;
      }
    }

    when(api.getEntries()).thenReturn(getEntries);
    when(api.getContent()).thenReturn(firstGetContent, nextGetContent);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(api)) {
      soft.assertThat(nessie.allContents(ref, ICEBERG_CONTENT_TYPES))
          .hasSize(CONTENT_BATCH_SIZE * 10)
          .containsExactlyInAnyOrderElementsOf(expected.entrySet());
    }

    verify(api, times(1)).getEntries();
    verify(api, times(10)).getContent();
  }
}
