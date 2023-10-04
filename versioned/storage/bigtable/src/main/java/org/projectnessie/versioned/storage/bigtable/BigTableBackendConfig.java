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
package org.projectnessie.versioned.storage.bigtable;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;

@Value.Immutable
public interface BigTableBackendConfig {

  /**
   * The main data client. This client is used for reads, and also writes if {@link
   * #singleClusterDataClient()} is not present.
   */
  BigtableDataClient dataClient();

  /**
   * The client to use for conditional writes, if present. If not present, {@link #dataClient()} is
   * used for writes as well.
   *
   * <p>Conditional writes must use a single-cluster application profile.
   */
  @Default
  default BigtableDataClient singleClusterDataClient() {
    return dataClient();
  }

  @Nullable
  @jakarta.annotation.Nullable
  BigtableTableAdminClient tableAdminClient();

  Optional<String> tablePrefix();

  static ImmutableBigTableBackendConfig.Builder builder() {
    return ImmutableBigTableBackendConfig.builder();
  }
}
