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
package org.projectnessie.versioned.storage.common.persist;

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_ID_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_REFERENCED_KEY;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.immutables.value.Value;

public interface Obj {

  /** The ID of this object. */
  @JsonIgnore
  @JacksonInject(OBJ_ID_KEY)
  ObjId id();

  @JsonIgnore
  ObjType type();

  /**
   * Contains the timestamp in microseconds since epoch when the object was last written, only
   * intended for repository cleanup mechanisms.
   *
   * <p>The value of this attribute is generated exclusively by the {@link Persist} implementations.
   *
   * <p>This attribute is <em>not</em> consistent when using a caching {@link Persist}.
   *
   * <p>When <em>reading</em> an object, this value is either {@code 0}, which means that the object
   * was written using a Nessie version that did not have this attribute, or a (positive) timestamp
   * when the object was written.
   *
   * <p>When <em>storing</em> an object, this value <em>must</em> be {@code 0}. The only one
   * exception is for tests that exercise the relevant code paths - those tests do also use {@code
   * -1} as a sentinel to write "NULL".
   *
   * <p>In any case it is <em>illegal</em> to refer to and/or interpret this attribute from code
   * that does not have to deal explicitly with this value, only code that runs maintenance
   * operations shall use this value.
   */
  @JsonIgnore
  @JacksonInject(OBJ_REFERENCED_KEY)
  @Value.Default
  @Value.Auxiliary
  default long referenced() {
    return 0L;
  }

  // no generics, we're good
  Obj withReferenced(long referenced);
}
