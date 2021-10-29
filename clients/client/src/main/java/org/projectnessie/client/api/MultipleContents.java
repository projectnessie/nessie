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
package org.projectnessie.client.api;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.projectnessie.error.NessieContentsNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;

public interface MultipleContents extends Map<ContentsKey, Contents> {

  /**
   * Returns the {@link Contents} object by its key assuming it is present.
   *
   * <p>This method is similar to {@link Map#get(Object)}, but it throws an exception on trying to
   * retrieve missing contents.
   *
   * @param key the contents key to retrieve
   * @return a non-null {@link Contents} object for the specified key.
   * @throws NessieContentsNotFoundException if no contents are present for the specified key.
   */
  @Nonnull
  Contents value(ContentsKey key) throws NessieContentsNotFoundException;

  /**
   * Extracts the contents value of the specified type, if possible.
   *
   * <p>An empty {@link Optional} is returned if the key is not present, or if the contents under
   * that key do not match the specified value type.
   *
   * @param key the contents key to retrieve
   * @param valueClass the type of the value to extract
   * @return a non-null {@link Optional} object for the extracted value.
   * @see Contents#unwrap(Class)
   */
  @Nonnull
  <T extends Contents> Optional<T> getAs(ContentsKey key, Class<T> valueClass);

  /**
   * Extracts the contents value of the specified type assuming it is present.
   *
   * <p>A {@link NessieContentsNotFoundException} is thrown if the key is not present, or if the
   * contents under that key do not match the specified value type.
   *
   * @param key the contents key to retrieve
   * @param valueClass the type of the value to extract
   * @return a non-null value object for the specified key and type.
   * @throws NessieContentsNotFoundException if no contents are present for the specified key or if
   *     the contents do not contain a value of the specified type.
   * @see #value(ContentsKey)
   * @see #getAs(ContentsKey, Class)
   */
  @Nonnull
  <T extends Contents> T valueAs(ContentsKey key, Class<T> valueClass)
      throws NessieContentsNotFoundException;

  /**
   * Extracts the contents value assuming it is present and the type matches the expected
   * compile-time value type.
   *
   * <p>A {@link NessieContentsNotFoundException} is thrown if the key is not present. However, if
   * the contents under that key do not match the implied value type a {@link ClassCastException}
   * will likely be raised at the call site.
   *
   * @param key the contents key to retrieve
   * @return a non-null value object for the specified key and type.
   * @throws NessieContentsNotFoundException if no contents are present for the specified key.
   * @see #value(ContentsKey)
   * @see #getAs(ContentsKey, Class)
   */
  @Nonnull
  <T extends Contents> T valueAs(ContentsKey key) throws NessieContentsNotFoundException;
}
