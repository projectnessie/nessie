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
package org.projectnessie.versioned.storage.common.persist;

import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.Compression;

@Value.Immutable
public interface PersistOptions {

  PersistOptions DEFAULT = PersistOptions.builder().build();

  PersistOptions NO_SIZE_RESTRICTIONS =
      PersistOptions.builder().ignoreSoftSizeRestrictions(true).build();

  /** Whether to explicitly ignore soft size restrictions; use {@code false}, if in doubt. */
  @Value.Default
  default boolean ignoreSoftSizeRestrictions() {
    return false;
  }

  /**
   * Jackson JSON view to use for custom objects. This per-call view is passed to the Jackson object
   * mapper when serializing custom objects.
   *
   * @see com.fasterxml.jackson.databind.ObjectMapper#writerWithView(Class)
   */
  Optional<Class<?>> jsonView();

  /**
   * Context to use for custom objects. This per-call context is passed to the Jackson object mapper
   * when serializing custom objects.
   *
   * @see com.fasterxml.jackson.databind.ObjectMapper#writer(ContextAttributes)
   */
  @Value.Default
  default Map<Object, Object> jsonContext() {
    return Collections.emptyMap();
  }

  /**
   * Compression to use when serializing custom objects. Note that the persistence layer may choose
   * not to use compression, if the object is too small.
   */
  @Value.Default
  default Compression compression() {
    return Compression.SNAPPY;
  }

  static ImmutablePersistOptions.Builder builder() {
    return ImmutablePersistOptions.builder();
  }
}
