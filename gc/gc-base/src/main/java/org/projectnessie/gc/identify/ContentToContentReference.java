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
package org.projectnessie.gc.identify;

import jakarta.validation.constraints.NotNull;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

/** Converts a content-object at a specific commit/content-key to a content-reference. */
@FunctionalInterface
public interface ContentToContentReference {
  ContentReference contentToReference(
      @NotNull Content content, @NotNull String commitId, @NotNull ContentKey key);
}
