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
package org.projectnessie.client.api;

import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;

/**
 * Request builder for "get content".
 *
 * @since {@link NessieApiV1}
 */
public interface GetContentBuilder extends OnReferenceBuilder<GetContentBuilder> {
  GetContentBuilder key(@Valid @jakarta.validation.Valid ContentKey key);

  GetContentBuilder keys(List<ContentKey> keys);

  /**
   * Set to {@code true} to perform access checks for write/create access in addition to read access
   * checks.
   */
  default GetContentBuilder forWrite(boolean forWrite) {
    if (forWrite) {
      throw new UnsupportedOperationException(
          getClass().getName() + " needs to implement forWrite(boolean)");
    }
    return this;
  }

  ContentResponse getSingle(@Valid @jakarta.validation.Valid ContentKey key)
      throws NessieNotFoundException;

  Map<ContentKey, Content> get() throws NessieNotFoundException;

  GetMultipleContentsResponse getWithResponse() throws NessieNotFoundException;
}
