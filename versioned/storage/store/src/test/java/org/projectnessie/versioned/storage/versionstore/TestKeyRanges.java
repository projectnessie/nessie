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
package org.projectnessie.versioned.storage.versionstore;

import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.versionstore.KeyRanges.keyRanges;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.MAIN_UNIVERSE;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKeyVariant;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestKeyRanges {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void entriesWrongParameters() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> keyRanges(null, ContentKey.of("foo"), ContentKey.of("foo"), ContentKey.of("foo")))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> keyRanges(null, null, ContentKey.of("foo"), ContentKey.of("foo")))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> keyRanges(null, ContentKey.of("foo"), null, ContentKey.of("foo")))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
  }

  @Test
  public void pagingTokenSupersedesMinKey() {
    String token =
        pagingToken(copyFromUtf8(keyToStoreKey(ContentKey.of("foo")).rawString())).asString();

    soft.assertThat(keyRanges(token, ContentKey.of("bar"), null, null))
        .extracting(KeyRanges::beginStoreKey)
        .isEqualTo(keyToStoreKey(ContentKey.of("foo")));

    soft.assertThat(keyRanges(token, null, null, ContentKey.of("bar")))
        .extracting(KeyRanges::beginStoreKey)
        .isEqualTo(keyToStoreKey(ContentKey.of("foo")));
  }

  @Test
  public void max() {
    ContentKey bar = ContentKey.of("bar");
    KeyRanges keyRange = keyRanges(null, null, bar, null);

    soft.assertThat(keyRange.endStoreKey()).isGreaterThan(keyToStoreKey(bar));
    soft.assertThat(keyRange.endStoreKey()).isGreaterThan(keyToStoreKeyVariant(bar, "Z"));
    soft.assertThat(keyRange.endStoreKey()).isGreaterThan(keyToStoreKeyVariant(bar, "A"));

    soft.assertThat(keyRange.endStoreKey()).isLessThan(keyToStoreKey(ContentKey.of("bar", "baz")));
    soft.assertThat(keyRange.endStoreKey()).isLessThan(keyToStoreKey(ContentKey.of("bar", "0")));
    soft.assertThat(keyRange.endStoreKey()).isLessThan(keyToStoreKey(ContentKey.of("barbaz")));
    soft.assertThat(keyRange.endStoreKey())
        .isLessThan(keyToStoreKey(ContentKey.of("barπ"))); //  UNICODE CHAR
    soft.assertThat(keyRange.endStoreKey()).isLessThan(keyToStoreKey(ContentKey.of("bar0")));

    soft.assertThat(keyRange.endStoreKey()).isLessThan(keyToStoreKey(ContentKey.of("bar0")));

    String storeKeyRawPrefix = MAIN_UNIVERSE + (char) 0 + "bar";

    soft.assertThat(keyRange.endStoreKey()).isGreaterThan(keyFromString(storeKeyRawPrefix));
    soft.assertThat(keyRange.endStoreKey())
        .isLessThan(keyFromString(storeKeyRawPrefix + (char) 1 + "baz"));
  }
}
