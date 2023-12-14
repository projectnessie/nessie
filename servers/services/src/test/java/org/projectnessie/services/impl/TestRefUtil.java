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

import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

class TestRefUtil {

  public static final String HASH_VALUE = "deadbeeffeedcafe";
  public static final String REF_NAME = "foo";

  @Test
  void toNamedRef() {
    assertThat(RefUtil.toNamedRef(Branch.of(REF_NAME, HASH_VALUE)))
        .isEqualTo(BranchName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Tag.of(REF_NAME, HASH_VALUE))).isEqualTo(TagName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(Detached.of(HASH_VALUE))).isSameAs(DetachedRef.INSTANCE);

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
                      public ReferenceType getType() {
                        return null;
                      }
                    }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported reference 'org.projectnessie.services.impl.TestRefUtil");
  }

  @Test
  void toNamedRefTyped() {
    assertThat(RefUtil.toNamedRef(ReferenceType.BRANCH, REF_NAME))
        .isEqualTo(BranchName.of(REF_NAME));
    assertThat(RefUtil.toNamedRef(ReferenceType.TAG, REF_NAME)).isEqualTo(TagName.of(REF_NAME));
  }

  @Test
  void toNamedRefTypedErrors() {
    assertThatThrownBy(() -> RefUtil.toNamedRef(null, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("referenceType must not be null");
  }

  @Test
  void toReferenceString() {
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), HASH_VALUE))
        .isEqualTo(Branch.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), HASH_VALUE))
        .isEqualTo(Tag.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(DetachedRef.INSTANCE, HASH_VALUE))
        .isEqualTo(Detached.of(HASH_VALUE));
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), (String) null))
        .isEqualTo(Branch.of(REF_NAME, null));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), (String) null))
        .isEqualTo(Tag.of(REF_NAME, null));
  }

  @Test
  void toReferenceHash() {
    assertThat(RefUtil.toReference(BranchName.of(REF_NAME), Hash.of(HASH_VALUE)))
        .isEqualTo(Branch.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(TagName.of(REF_NAME), Hash.of(HASH_VALUE)))
        .isEqualTo(Tag.of(REF_NAME, HASH_VALUE));
    assertThat(RefUtil.toReference(DetachedRef.INSTANCE, Hash.of(HASH_VALUE)))
        .isEqualTo(Detached.of(HASH_VALUE));
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
            "Unsupported named reference 'org.projectnessie.services.impl.TestRefUtil");
    assertThatThrownBy(() -> toReference(() -> REF_NAME, HASH_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.TestRefUtil");
    assertThatThrownBy(() -> toReference(DetachedRef.INSTANCE, (String) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("hash must not be null for detached references");
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
            "Unsupported named reference 'org.projectnessie.services.impl.TestRefUtil");
    assertThatThrownBy(() -> toReference(() -> REF_NAME, Hash.of(HASH_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Unsupported named reference 'org.projectnessie.services.impl.TestRefUtil");
    assertThatThrownBy(() -> toReference(DetachedRef.INSTANCE, (Hash) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("hash must not be null for detached references");
  }
}
