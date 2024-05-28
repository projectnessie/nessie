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
package org.projectnessie.catalog.formats.iceberg.manifest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import org.apache.avro.Schema;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface AvroReadWriteContext {
  static Builder builder() {
    return ImmutableAvroReadWriteContext.builder();
  }

  Map<String, Schema> schemaOverrides();

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder putSchemaOverride(String key, Schema value);

    @CanIgnoreReturnValue
    Builder putSchemaOverride(Map.Entry<String, ? extends Schema> entry);

    @CanIgnoreReturnValue
    Builder schemaOverrides(Map<String, ? extends Schema> entries);

    @CanIgnoreReturnValue
    Builder putAllSchemaOverrides(Map<String, ? extends Schema> entries);

    AvroReadWriteContext build();
  }
}
