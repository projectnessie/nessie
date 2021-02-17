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
package org.projectnessie.client;

import javax.validation.constraints.NotNull;

import org.projectnessie.api.ContentsApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;

/**
 * Extends ContentsApi with some helper methods for clients.
 */
public interface ClientContentsApi extends ContentsApi {

  void setContents(@NotNull ContentsKey key, String branch, @NotNull String hash, CommitMeta commitMeta,
                          @NotNull Contents contents) throws NessieNotFoundException, NessieConflictException;

  void deleteContents(ContentsKey key, String branch, String hash, CommitMeta commitMeta)
      throws NessieNotFoundException, NessieConflictException;
}
