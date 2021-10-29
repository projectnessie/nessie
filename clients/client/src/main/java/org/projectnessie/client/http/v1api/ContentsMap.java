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
package org.projectnessie.client.http.v1api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.projectnessie.client.api.MultipleContents;
import org.projectnessie.error.NessieContentsNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;

public class ContentsMap extends HashMap<ContentsKey, Contents> implements MultipleContents {

  private ContentsMap(Map<? extends ContentsKey, ? extends Contents> m) {
    super(m);
  }

  public static MultipleContents of(Collection<ContentsWithKey> contents) {
    return new ContentsMap(
        contents.stream()
            .collect(Collectors.toMap(ContentsWithKey::getKey, ContentsWithKey::getContents)));
  }

  @Nonnull
  @Override
  public Contents value(ContentsKey key) throws NessieContentsNotFoundException {
    Contents contents = get(key);
    if (contents == null) {
      throw new NessieContentsNotFoundException(key);
    }

    return contents;
  }

  @Nonnull
  @Override
  public <T extends Contents> Optional<T> getAs(ContentsKey key, Class<T> valueClass) {
    return Optional.ofNullable(get(key)).flatMap(c -> c.unwrap(valueClass));
  }

  @Nonnull
  @Override
  public <T extends Contents> T valueAs(ContentsKey key, Class<T> valueClass)
      throws NessieContentsNotFoundException {
    Contents contents = value(key);
    Optional<T> value = contents.unwrap(valueClass);
    if (!value.isPresent()) {
      throw new NessieContentsNotFoundException(key, valueClass);
    }

    return value.get();
  }

  @Nonnull
  @Override
  public <T extends Contents> T valueAs(ContentsKey key) throws NessieContentsNotFoundException {
    //noinspection unchecked
    return (T) valueAs(key, Contents.class);
  }
}
