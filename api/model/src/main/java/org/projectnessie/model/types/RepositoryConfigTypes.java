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
package org.projectnessie.model.types;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nonnull;
import org.projectnessie.model.RepositoryConfig;

/**
 * Provides the registry for all {@link RepositoryConfig repository config types}.
 *
 * <p>Repository config types are loaded via {@link RepositoryConfigTypeBundle}s using Java's {@link
 * ServiceLoader service loader} mechanism.
 */
public final class RepositoryConfigTypes {

  /** Retrieve an array of all registered repository config types. */
  public static RepositoryConfig.Type[] all() {
    return Registry.all();
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public static RepositoryConfig.Type forName(String name) {
    return Registry.forName(name);
  }

  static final class RegistryHelper implements RepositoryConfigTypeRegistry {

    private final List<RepositoryConfig.Type> list = new ArrayList<>();
    private final Map<String, RepositoryConfig.Type> names = new HashMap<>();

    @Override
    public void register(Class<? extends RepositoryConfig> type) {
      requireNonNull(type, "Illegal repository config type registration: type must not be null");

      JsonTypeName jsonTypeName = type.getAnnotation(JsonTypeName.class);
      if (jsonTypeName == null) {
        throw new IllegalArgumentException(
            String.format(
                "Repository config type registration: %s has no @JsonTypeName annotation",
                type.getName()));
      }

      String name = jsonTypeName.value();
      if (name == null || name.trim().isEmpty() || !name.trim().equals(name)) {
        throw new IllegalArgumentException(
            String.format(
                "Illegal repository config type registration: illegal name '%s' for %s",
                name, type.getName()));
      }
      RepositoryConfig.Type repositoryConfigType = new RepositoryConfigTypeImpl(name, type);

      RepositoryConfig.Type ex = names.get(name);
      if (ex != null) {
        throw new IllegalStateException(
            String.format(
                "Duplicate repository config type registration for %s/%s, existing: %s/%s",
                name, type.getName(), ex.name(), ex.type().getName()));
      }

      add(repositoryConfigType);
    }

    void add(RepositoryConfig.Type unknownRepositoryConfigType) {
      list.add(unknownRepositoryConfigType);
      names.put(unknownRepositoryConfigType.name(), unknownRepositoryConfigType);
    }
  }

  /**
   * Internal class providing the actual registry. This is a separate class to implicitly use lazy
   * initialization.
   */
  private static final class Registry {

    private static final RepositoryConfig.Type[] all;
    private static final Map<String, RepositoryConfig.Type> byName;

    static {
      RegistryHelper registryHelper = new RegistryHelper();

      // Add the "DEFAULT" type.
      RepositoryConfig.Type unknownRepositoryConfigType = new DefaultRepositoryConfigTypeImpl();
      registryHelper.add(unknownRepositoryConfigType);

      for (RepositoryConfigTypeBundle bundle :
          ServiceLoader.load(RepositoryConfigTypeBundle.class)) {
        bundle.register(registryHelper);
      }

      byName = Collections.unmodifiableMap(registryHelper.names);
      all = registryHelper.list.toArray(new RepositoryConfig.Type[0]);
    }

    private static RepositoryConfig.Type[] all() {
      return all.clone();
    }

    private static RepositoryConfig.Type forName(String name) {
      RepositoryConfig.Type type = byName.get(name);
      if (type == null) {
        throw new IllegalArgumentException("No repository config type registered for name " + name);
      }
      return type;
    }
  }

  /** Internally used wrapper for a {@link RepositoryConfig.Type}. */
  private static final class RepositoryConfigTypeImpl implements RepositoryConfig.Type {

    private final String name;

    private final Class<? extends RepositoryConfig> type;

    private RepositoryConfigTypeImpl(String name, Class<? extends RepositoryConfig> type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Class<? extends RepositoryConfig> type() {
      return type;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RepositoryConfigTypeImpl)) {
        return false;
      }
      RepositoryConfigTypeImpl that = (RepositoryConfigTypeImpl) o;
      return name.equals(that.name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }

  /** Internally used wrapper for the {@code UNKNOWN} repository config type. */
  private static final class DefaultRepositoryConfigTypeImpl implements RepositoryConfig.Type {
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String toString() {
      return name();
    }

    @Override
    public String name() {
      return "UNKNOWN";
    }

    @Override
    public Class<? extends RepositoryConfig> type() {
      throw new IllegalStateException("UNKNOWN RepositoryConfig.Type has no type");
    }
  }
}
