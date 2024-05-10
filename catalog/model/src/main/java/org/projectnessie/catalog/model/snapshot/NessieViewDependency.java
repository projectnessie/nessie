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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Describes a dependency of a {@link NessieViewSnapshot}.
 *
 * <p>A view dependency can be the snapshot of a {@link NessieTableSnapshot table} or another {@link
 * NessieViewSnapshot view}.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieViewDependency.class)
@JsonDeserialize(as = ImmutableNessieViewDependency.class)
public interface NessieViewDependency {
  @Nullable
  @jakarta.annotation.Nullable
  NessieId tableSnapshot();

  @Nullable
  @jakarta.annotation.Nullable
  NessieId viewSnapshot();

  List<NessieId> referencedColumns();
}
