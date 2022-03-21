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
package org.projectnessie.gc.base;

import com.google.common.hash.BloomFilter;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Content;

/** Intermediate object to hold the per reference results from identify live contents gc step. */
@Value.Immutable
public interface LiveContentsResult extends Serializable {

  @Nullable
  BloomFilter<Content> getBloomFilter();

  String getReferenceName();

  String getHashOnReference();
}
