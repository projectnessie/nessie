/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.cmdspec;

import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.nessie.cli.grammar.Node;

@Value.Immutable
public interface RevertContentCommandSpec extends RefWithTypeCommandSpec, CatalogAware {
  default CommandType commandType() {
    return CommandType.REVERT_CONTENT;
  }

  @Nullable
  @Override
  String getInCatalog();

  @Nullable
  @Override
  @Value.Default
  default Node sourceNode() {
    return RefWithTypeCommandSpec.super.sourceNode();
  }

  @Value.Default
  default boolean isDryRun() {
    return false;
  }

  @Value.Default
  default boolean isAllowDeletes() {
    return false;
  }

  @Nullable
  @Override
  String getRef();

  @Nullable
  String getRefType();

  @Nullable
  String getSourceRef();

  @Nullable
  String getSourceRefType();

  @Nullable
  String getSourceRefTimestampOrHash();

  List<String> getContentKeys();
}
