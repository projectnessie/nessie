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
package org.projectnessie.versioned.tiered.adapter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseAdapterConfiguration {

  private final Map<String, Object> attributes = new HashMap<>();

  @SuppressWarnings("unchecked")
  <X> X getAttribute(ConfigItem<X> configItem) {
    return (X) attributes.get(configItem.getKey());
  }

  <X> void setAttribute(ConfigItem<X> configItem, X value) {
    attributes.put(configItem.getKey(), value);
  }

  public static class ConfigItem<T> {
    private final String key;
    private final T defaultValue;
    private final boolean mandatory;

    public ConfigItem(String key, T defaultValue, boolean mandatory) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.mandatory = mandatory;
    }

    public String getKey() {
      return key;
    }

    public T getDefaultValue() {
      return defaultValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConfigItem<?> configItem = (ConfigItem<?>) o;
      return Objects.equals(key, configItem.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }

    @Override
    public String toString() {
      return "Attribute{" + "key='" + key + '\'' + '}';
    }

    public T get(DatabaseAdapterConfiguration config) {
      T v = config.getAttribute(this);
      if (v == null) {
        v = fallbackValue();
      }
      if (v == null) {
        v = getDefaultValue();
      }
      if (mandatory && v == null) {
        throw new IllegalStateException(
            String.format("Config item '%s' is mandatory, but not configured.", key));
      }
      return v;
    }

    protected T fallbackValue() {
      return null;
    }

    public void set(DatabaseAdapterConfiguration config, T value) {
      config.setAttribute(this, value);
    }
  }

  public static class IntegerConfigItem extends ConfigItem<Integer> {
    public IntegerConfigItem(String key, Integer defaultValue, boolean mandatory) {
      super(key, defaultValue, mandatory);
    }

    @Override
    protected Integer fallbackValue() {
      String s = System.getProperty(getKey());
      return s != null ? Integer.parseInt(s) : null;
    }
  }

  public static class StringConfigItem extends ConfigItem<String> {
    public StringConfigItem(String key, String defaultValue, boolean mandatory) {
      super(key, defaultValue, mandatory);
    }

    @Override
    protected String fallbackValue() {
      return System.getProperty(getKey());
    }
  }

  public static class DirectoryConfigItem extends ConfigItem<Path> {
    public DirectoryConfigItem(String key, Path defaultValue, boolean mandatory) {
      super(key, defaultValue, mandatory);
    }

    @Override
    protected Path fallbackValue() {
      String s = System.getProperty(getKey());
      return s != null ? Paths.get(s) : null;
    }
  }
}
