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
package org.projectnessie.versioned;

import javax.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

/**
 * A PUT operation provided by the client in order to set a new value in a commit. Can optionally
 * declare whether the prior hash must match.
 */
@Value.Immutable
public interface Put extends Operation {

  /**
   * The value to store for this operation.
   *
   * @return the value
   */
  Content getValue();

  /**
   * Creates a put-operation for the given key and value.
   *
   * <p>{@code value} with a {@code null} content ID is <em>required</em> when creating/adding new
   * content.
   *
   * <p>{@code value} with a non-{@code null} content ID is <em>required</em> when updating existing
   * content.
   *
   * <p>A content object is considered to be the same using the {@link ContentKey content-key} and
   * the {@link Content#getId() content-id}.
   *
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  static Put of(
      @Nonnull @jakarta.annotation.Nonnull ContentKey key,
      @Nonnull @jakarta.annotation.Nonnull Content value) {
    return ImmutablePut.builder().key(key).value(value).build();
  }
}
