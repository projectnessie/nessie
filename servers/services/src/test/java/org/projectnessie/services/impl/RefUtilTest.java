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
package org.projectnessie.services.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.services.impl.RefUtil.toReference;

import javax.annotation.Nonnull;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transaction;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.TransactionName;

class RefUtilTest {

  public static final String HASH_VALUE = "deadbeeffeedcafe";
  public static final String REF_NAME = "foo";

  @Test
  void toNamedRef() {
    assertThat(RefUtil.toNamedRef(Transaction.of(REF_NAME, HASH_VALUE)))
      .isEqualTo(TransactionName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Branch.of(REF_NAME, HASH_VALUE)))
        .isEqualTo(BranchName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Tag.of(REF_NAME, HASH_VALUE))).isEqualTo(TagName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Transaction.of(REF_NAME, null)))
      .isEqualTo(TransactionName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Branch.of(REF_NAME, null))).isEqualTo(BranchName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Tag.of(REF_NAME, null))).isEqualTo(TagName.of(REF_NAME));
  }

  @Test
  void toNamedRefErrors() {
    assertThatThrownBy(() -> RefUtil.toNamedRef(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("reference must not be null");
    assertThatThrownBy(
            () ->
                RefUtil.toNamedRef(
                    new Reference() {
                      @Override
                      public String getName() {
                        return null;
                      }

                      @Override
                      public String getHash() {
                        return null;
                      }

                      @Override
                      public ReferenceMetadata getMetadata() {
                        return null;
                      }

                      @Override
                      public Reference withHash(String hash) {
                        return null;
                      }
                    }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported reference 'org.projectnessie.services.impl.RefUtilTest");
  }

  @Test
  void toReferenceString() {
    assertThat(RefUtil.toReference(TransactionName.of(REF_NAME), HASH_VALUE))
      .isEqualTo(Transaction.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), HASH_VALUE))
        .isEqualTo(Branch.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), HASH_VALUE))
        .isEqualTo(Tag.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TransactionName.of(REF_NAME), (String) null))
      .isEqualTo(Transaction.of(REF_NAME, null));
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), (String) null))
        .isEqualTo(Branch.of(REF_NAME, null));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), (String) null))
        .isEqualTo(Tag.of(REF_NAME, null));
  }

  @Test
  void toReferenceHash() {
    assertThat(RefUtil.toReference(TransactionName.of(REF_NAME), Hash.of(HASH_VALUE)))
      .isEqualTo(Transaction.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), Hash.of(HASH_VALUE)))
        .isEqualTo(Branch.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), Hash.of(HASH_VALUE)))
        .isEqualTo(Tag.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TransactionName.of(REF_NAME), (Hash) null))
      .isEqualTo(Transaction.of(REF_NAME, null));
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), (Hash) null))
        .isEqualTo(Branch.of(REF_NAME, null));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), (Hash) null))
        .isEqualTo(Tag.of(REF_NAME, null));
  }

  @Test
  void toReferenceStringErrors() {
    assertThatThrownBy(() -> toReference(null, (String) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namedRef must not be null");
    assertThatThrownBy(() -> toReference(null, HASH_VALUE))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namedRef must not be null");
    assertThatThrownBy(
            () ->
                toReference(
                    new NamedRef() {
                      @Override
                      @Nonnull
                      public String getName() {
                        return REF_NAME;
                      }
                    },
                    HASH_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.RefUtilTest");
    assertThatThrownBy(() -> toReference(() -> REF_NAME, HASH_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.RefUtilTest");
  }

  @Test
  void toReferenceHashErrors() {
    assertThatThrownBy(() -> toReference(null, (Hash) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namedRef must not be null");
    assertThatThrownBy(() -> toReference(null, Hash.of(HASH_VALUE)))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namedRef must not be null");
    assertThatThrownBy(
            () ->
                toReference(
                    new NamedRef() {
                      @Override
                      @Nonnull
                      public String getName() {
                        return REF_NAME;
                      }
                    },
                    Hash.of(HASH_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.RefUtilTest");
    assertThatThrownBy(() -> toReference(() -> REF_NAME, Hash.of(HASH_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.RefUtilTest");
  }
}
