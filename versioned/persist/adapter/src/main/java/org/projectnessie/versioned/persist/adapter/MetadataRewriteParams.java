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
package org.projectnessie.versioned.persist.adapter;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.function.Function;
import org.immutables.value.Value;

public interface MetadataRewriteParams extends ToBranchParams {

  /** Whether to keep the individual commits and do not squash the commits to merge. */
  @Value.Default
  default boolean keepIndividualCommits() {
    return false;
  }

  /** Function to rewrite the commit-metadata. */
  Function<List<ByteString>, ByteString> getUpdateCommitMetadata();
}
