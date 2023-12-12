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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_ID_KEY;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.base.Preconditions;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * A generic {@link Obj} that can be used to store any JSON-serializable object.
 *
 * <p>A typical implementation using Java Immutables is:
 *
 * <pre>{@code
 * @Value.Immutable
 * @JsonSerialize(as = ImmutableJsonTestObj.class)
 * @JsonDeserialize(as = ImmutableJsonTestObj.class)
 * public interface MyJsonObj extends JsonObj<MyJsonModel> {
 *
 *   @Override
 *   default ObjType type() {
 *     return MyJsonType.INSTANCE;
 *   }
 *
 *   @Override
 *   @JsonUnwrapped
 *   MyJsonModel model();
 * }
 * }</pre>
 */
public interface JsonObj<T> extends Obj {

  /**
   * {@inheritDoc}
   *
   * @implNote if this method is overridden, care should be taken to also copy the
   *     {@code @JsonIgnore} and {@code @JacksonInject} annotations to the overridden method, as
   *     these are not automatically inherited.
   */
  @JsonIgnore
  @JacksonInject(OBJ_ID_KEY)
  @Override
  ObjId id();

  /**
   * The model object to be serialized as JSON.
   *
   * @implNote subclasses may wish to override this method to provide a more specific return type,
   *     although that is not required by Jackson serialization (but it is for Java Immutables). If
   *     they do so, they should also copy the {@code @JsonUnwrapped} annotation to the overridden
   *     method as it is not automatically inherited.
   */
  @JsonUnwrapped
  T model();

  @Value.Check
  default void check() {
    Preconditions.checkState(
        type().targetClass().isAssignableFrom(getClass()),
        "Type %s is not assignable from %s",
        type(),
        getClass());
  }
}
